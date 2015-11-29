/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A memstore implementation which supports in-memory compaction.
 * A compaction pipeline is added between the active segment and the snapshot segment;
 * it consists of a list of segments that are subject to compaction.
 * By compaction we mean removing overwritten entries that are not needed by scans.
 * Like the snapshot, all pipeline segments are read-only; updates only affect the active segment.
 * To ensure this property we take advantage of the existing blocking mechanism -- the active
 * segment is pushed to the pipeline while holding updatesLock in exclusive mode.
 * Periodically, a compaction is applied in the background to all pipeline segments resulting
 * in a single read-only segment. The ``old'' components are discarded when no scanner is reading
 * them.
 */
@InterfaceAudience.Private
public class CompactingMemStore extends AbstractMemStore {
  public final static long DEEP_OVERHEAD_PER_PIPELINE_ITEM = ClassSize.align(
      ClassSize.TIMERANGE_TRACKER +
          ClassSize.CELL_SKIPLIST_SET + ClassSize.CONCURRENT_SKIPLISTMAP);

  private static final Log LOG = LogFactory.getLog(CompactingMemStore.class);
  private HRegion region;
  private Store store;
  private CompactionPipeline pipeline;
  private MemStoreCompactor compactor;
  private NavigableMap<Long, Long> timestampToWALSeqId;
  // the threshold on active size for in-memory flush
  private long flushSizeLowerBound;
  private final AtomicBoolean inMemoryFlushInProgress = new AtomicBoolean(false);
  // the flag for tests only
  private final AtomicBoolean allowCompaction = new AtomicBoolean(true);

  public CompactingMemStore(Configuration conf, CellComparator c,
      HRegion region, Store store) throws IOException {
    super(conf, c);
    this.region = region;
    this.store = store;
    this.pipeline = new CompactionPipeline(getHRegion());
    this.compactor = new MemStoreCompactor();
    this.timestampToWALSeqId = new TreeMap<>();
    initFlushSizeLowerBound(conf);
  }

  private void initFlushSizeLowerBound(Configuration conf) {
    String flushedSizeLowerBoundString =
        getHRegion().getTableDesc().getValue(FlushLargeStoresPolicy
            .HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND);
    if (flushedSizeLowerBoundString == null) {
      flushSizeLowerBound =
          conf.getLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND,
              FlushLargeStoresPolicy.DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND);
      if (LOG.isDebugEnabled()) {
        LOG.debug(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND
            + " is not specified, use global config(" + flushSizeLowerBound + ") instead");
      }
    } else {
      try {
        flushSizeLowerBound = Long.parseLong(flushedSizeLowerBoundString);
      } catch (NumberFormatException nfe) {
        flushSizeLowerBound =
            conf.getLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND,
                FlushLargeStoresPolicy.DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND);
        LOG.warn("Number format exception when parsing "
            + FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND + " for table "
            + getHRegion().getTableDesc().getTableName() + ":" + flushedSizeLowerBoundString + ". "
            + nfe + ", use global config(" + flushSizeLowerBound + ") instead");
      }
    }
  }

  public static long getStoreSegmentSize(StoreSegment segment) {
    return segment.getSize() - DEEP_OVERHEAD_PER_PIPELINE_ITEM;
  }

  public static long getStoreSegmentListSize(LinkedList<? extends StoreSegment> list) {
    long res = 0;
    for (StoreSegment segment : list) {
      res += getStoreSegmentSize(segment);
    }
    return res;
  }

  /**
   * @return Total memory occupied by this MemStore.
   * This is not thread safe and the memstore may be changed while computing its size.
   * It is the responsibility of the caller to make sure this doesn't happen.
   */
  @Override
  public long size() {
    long res = 0;
    for (StoreSegment item : getListOfSegments()) {
      res += item.getSize();
    }
    return res;
  }

  /**
   * Push the current active memstore segment into the pipeline
   * and create a snapshot of the tail of current compaction pipeline
   * Snapshot must be cleared by call to {@link #clearSnapshot}.
   * {@link #clearSnapshot(long)}.
   * @param flushOpSeqId the sequence id that is attached to the flush operation in the wal
   *
   * @return {@link MemStoreSnapshot}
   */
  @Override
  public MemStoreSnapshot snapshot(long flushOpSeqId) {
    MutableSegment active = getActive();
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!getSnapshot().isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      LOG.info("FLUSHING TO DISK: region "+ getHRegion().getRegionInfo().getRegionNameAsString()
          + "store: "+ Bytes.toString(getFamilyName()));
      stopCompact();
      pushActiveToPipeline(active, flushOpSeqId, false);
      this.snapshotId = EnvironmentEdgeManager.currentTime();
      pushTailToSnapshot();
    }
    return new MemStoreSnapshot(this.snapshotId, getSnapshot());
  }

  @Override
  public void updateLowestUnflushedSequenceIdInWal(boolean onlyIfGreater) {
    long minTimestamp = pipeline.getMinTimestamp();
    Long seqId = truncateLowerTSsAndGetSeqId(minTimestamp);
    if (seqId == null) return;
    byte[] encodedRegionName = getHRegion().getRegionInfo().getEncodedNameAsBytes();
    byte[] familyName = getFamilyName();
    WAL wal = getHRegion().getWAL();
    if (wal != null) {
      wal.updateStore(encodedRegionName, familyName, seqId, onlyIfGreater);
    }
  }

  /**
   * Remove n key from the memstore. Only kvs that have the same key and the same memstoreTS are
   * removed. It is ok to not update timeRangeTracker in this call.
   *
   * @param cell
   */
  @Override
  public void rollback(Cell cell) {
    rollbackSnapshot(cell);
    pipeline.rollback(cell);
    rollbackActive(cell);
  }

  @Override
  protected LinkedList<StoreSegment> getListOfSegments() {
    LinkedList<StoreSegment> pipelineList = pipeline.getStoreSegmentList();
    LinkedList<StoreSegment> list = new LinkedList<StoreSegment>();
    list.add(getActive());
    list.addAll(pipelineList);
    list.add(getSnapshot());
    return list;
  }

  @Override
  protected List<StoreSegmentScanner> getListOfScanners(long readPt) throws IOException {
    LinkedList<StoreSegment> pipelineList = pipeline.getStoreSegmentList();
    LinkedList<StoreSegmentScanner> list = new LinkedList<StoreSegmentScanner>();
    list.add(getActive().getScanner(readPt));
    for (StoreSegment item : pipelineList) {
      list.add(item.getScanner(readPt));
    }
    list.add(getSnapshot().getScanner(readPt));
    // set sequence ids by decsending order
    Iterator<StoreSegmentScanner> iterator = list.descendingIterator();
    int seqId = 0;
    while (iterator.hasNext()) {
      iterator.next().setSequenceID(seqId);
      seqId++;
    }
    return list;
  }

  /**
   * Check whether anything need to be done based on the current active set size.
   * The method is invoked on every addition to the active set.
   * For CompactingMemStore, flush the active set to the read-only memory if it's
   * size is above threshold
   */
  @Override
  protected void checkActiveSize() {
    if (shouldFlushInMemory()) {
      /* The thread is dispatched to flush-in-memory. This cannot be done
      * on the same thread, because for flush-in-memory we require updatesLock
      * in exclusive mode while this method (checkActiveSize) is invoked holding updatesLock
      * in the shared mode. */
      ExecutorService pool = getPool();
      if(pool != null) {
        InMemoryFlushWorker worker = new InMemoryFlushWorker();
        LOG.info("Dispatching the MemStore in-memory flush for store "
            + store.getColumnFamilyName());
        pool.submit(worker);
        inMemoryFlushInProgress.set(true);
      }
    }
  }

  // internal method, external only for tests
  // when invoked directly from tests it must be verified that the caller doesn't hold updatesLock,
  // otherwise there is a deadlock
  void flushInMemory() throws IOException {
    // Phase I: Update the pipeline
    getHRegion().lockUpdatesExcl();
    try {
      MutableSegment active = getActive();
      LOG.info("IN-MEMORY FLUSH: Pushing active segment into compaction pipeline, " +
          "and initiating compaction.");
      long flushOpSeqId = getHRegion().getWalSequenceId(getHRegion().getWAL());
      pushActiveToPipeline(active, flushOpSeqId, true);
    } finally {
      getHRegion().unlockUpdatesExcl();
    }
    // Phase II: Compact the pipeline
    try {
      if (allowCompaction.get()) {
        // setting the inMemoryFlushInProgress flag again for the case this method is invoked
        // directly (only in tests) in the common path setting from true to true is idempotent
        inMemoryFlushInProgress.set(true);
        // Speculative compaction execution, may be interrupted if flush is forced while
        // compaction is in progress
        compactor.startCompact();
      }
    } catch (IOException e) {
      LOG.warn("Unable to run memstore compaction. region "
          + getHRegion().getRegionInfo().getRegionNameAsString()
          + "store: "+ Bytes.toString(getFamilyName()), e);
    }
  }

  /**
   * Returns the (maximal) sequence id that is associated with the maximal ts that is smaller than
   * the given ts, and removes all entries in the ts=>seqid map with timestamp smaller than
   * the given ts.
   *
   * @param minTimestamp
   * @return sequence id
   */
  private Long truncateLowerTSsAndGetSeqId(long minTimestamp) {
    Long res = null;
    Long last = null;
    List<Long> tsToRemove = new LinkedList<Long>();
    // go through the timestamps by their order; stop when reaching the end or to a greater
    // timestamp than the given one. Return the seq id that is associated with *last* ts (if not
    // null) that is smaller than the given ts
    for (Long ts : timestampToWALSeqId.keySet()) {
      if (last != null) {
        tsToRemove.add(last);
      }
      if (ts >= minTimestamp) {
        break;
      }
      // else ts < min ts in memstore, therefore can use sequence id to truncate wal
      last = ts;
    }
    if (last != null) {
      res = timestampToWALSeqId.get(last);
    }
    for (Long ts : tsToRemove) {
      timestampToWALSeqId.remove(ts);
    }
    return res;
  }

  private HRegion getHRegion() {
    return region;
  }

  private byte[] getFamilyName() {
    return store.getFamily().getName();
  }

  private ExecutorService getPool() {
    RegionServerServices rs = getHRegion().getRegionServerServices();
    if(rs==null) return null;
    return rs.getExecutorService();
  }

  private boolean shouldFlushInMemory() {
    if(getActive().getSize() > 0.9*flushSizeLowerBound) { // size above flush threshold
      return (allowCompaction.get() && !inMemoryFlushInProgress.get());
    }
    return false;
  }

  /**
   * The request to cancel the compaction asynchronous task (caused by in-memory flush)
   * The compaction may still happen if the request was sent too late
   * Non-blocking request
   */
  private void stopCompact() {
    if (inMemoryFlushInProgress.get()) {
      compactor.stopCompact();
      inMemoryFlushInProgress.set(false);
    }
  }

  private void pushActiveToPipeline(MutableSegment active, long flushOpSeqId,
      boolean needToUpdateRegionMemStoreSizeCounter) {
    if (!active.isEmpty()) {
      pipeline.pushHead(active);
      active.setSize(active.getSize() - deepOverhead() + DEEP_OVERHEAD_PER_PIPELINE_ITEM);
      long size = getStoreSegmentSize(active);
      resetCellSet();
      updateRegionOverflowMemstoreSizeCounter(size); //push size into pipeline
      if (needToUpdateRegionMemStoreSizeCounter) {
        updateRegionMemStoreSizeCounter(-size);
      }
      Long now = EnvironmentEdgeManager.currentTime();
      timestampToWALSeqId.put(now, flushOpSeqId);
    }
  }

  private void pushTailToSnapshot() {
    ImmutableSegment tail = pipeline.pullTail();
    if (!tail.isEmpty()) {
      setSnapshot(tail);
      long size = getStoreSegmentSize(tail);
      setSnapshotSize(size);
      updateRegionOverflowMemstoreSizeCounter(-size); //pull size out of pipeline
    }
  }

  private void updateRegionOverflowMemstoreSizeCounter(long size) {
    if (getHRegion() != null) {
      long globalMemStoreOverflowSize = getHRegion().addAndGetGlobalMemstoreOverflowSize(size);
      // no need to update global memstore size as it is updated by the flusher
      LOG.debug(getHRegion().getRegionInfo().getEncodedName() + " globalMemStoreOverflowSize: " +
          globalMemStoreOverflowSize);
    }
  }

  private void updateRegionMemStoreSizeCounter(long size) {
    if (getHRegion() != null) {
      // need to update global memstore size when it is not accounted by the flusher
      long globalMemstoreSize = getHRegion().addAndGetGlobalMemstoreSize(size);
      LOG.debug(getHRegion().getRegionInfo().getEncodedName() + " globalMemstoreSize: " +
          globalMemstoreSize);
    }
  }



  /*----------------------------------------------------------------------
  * The in-memory-flusher thread performs the flush asynchronously.
  * There is at most one thread per memstore instance.
  * It takes the updatesLock exclusively, only to push active segment into the pipeline.
  * It then compacts the pipeline without holding the lock.
  */
  private class InMemoryFlushWorker extends EventHandler {

    public InMemoryFlushWorker () {
      super(getHRegion().getRegionServerServices(), EventType.RS_IN_MEMORY_FLUSH_AND_COMPACTION);
    }

    @Override
    public void process() throws IOException {
      flushInMemory();
    }
  }

  /**
   * The ongoing MemStore Compaction manager, dispatches a solo running compaction
   * and interrupts the compaction if requested.
   * The MemStoreScanner is used to traverse the compaction pipeline. The MemStoreScanner
   * is included in internal store scanner, where all compaction logic is implemented.
   * Threads safety: It is assumed that the compaction pipeline is immutable,
   * therefore no special synchronization is required.
   */
  private class MemStoreCompactor {

    private MemStoreScanner scanner;            // scanner for pipeline only
    // scanner on top of MemStoreScanner that uses ScanQueryMatcher
    private StoreScanner compactingScanner;

    // smallest read point for any ongoing MemStore scan
    private long smallestReadPoint;

    // a static version of the CellSetMgrs list from the pipeline
    private VersionedSegmentsList versionedList;
    private final AtomicBoolean isInterrupted = new AtomicBoolean(false);

    /**
     * ----------------------------------------------------------------------
     * The request to dispatch the compaction asynchronous task.
     * The method returns true if compaction was successfully dispatched, or false if there
     *
     * is already an ongoing compaction (or pipeline is empty).
     */
    public boolean startCompact() throws IOException {
      if (pipeline.isEmpty()) return false;             // no compaction on empty pipeline

      List<StoreSegmentScanner> scanners = new ArrayList<StoreSegmentScanner>();
      // get the list of segments from the pipeline
      this.versionedList = pipeline.getVersionedList();
      // the list is marked with specific version

      // create the list of scanners with maximally possible read point, meaning that
      // all KVs are going to be returned by the pipeline traversing
      for (StoreSegment segment : this.versionedList.getStoreSegments()) {
        scanners.add(segment.getScanner(Long.MAX_VALUE));
      }
      scanner =
          new MemStoreScanner(CompactingMemStore.this, scanners, Long.MAX_VALUE,
              MemStoreScanner.Type.COMPACT_FORWARD);

      smallestReadPoint = store.getSmallestReadPoint();
      compactingScanner = createScanner(store);

      LOG.info("Starting the MemStore in-memory compaction for store " +
          store.getColumnFamilyName());

      doCompact();
      return true;
    }

    /*----------------------------------------------------------------------
    * The request to cancel the compaction asynchronous task
    * The compaction may still happen if the request was sent too late
    * Non-blocking request
    */
    public void stopCompact() {
      isInterrupted.compareAndSet(false, true);
    }


    /*----------------------------------------------------------------------
    * Close the scanners and clear the pointers in order to allow good
    * garbage collection
    */
    private void releaseResources() {
      isInterrupted.set(false);
      scanner.close();
      scanner = null;
      compactingScanner.close();
      compactingScanner = null;
      versionedList = null;
    }

    /*----------------------------------------------------------------------
    * The worker thread performs the compaction asynchronously.
    * The solo (per compactor) thread only reads the compaction pipeline.
    * There is at most one thread per memstore instance.
    */
    private void doCompact() {

      ImmutableSegment result = StoreSegmentFactory.instance()  // create the scanner
          .createImmutableSegment(getConfiguration(), getComparator(),
              CompactingMemStore.DEEP_OVERHEAD_PER_PIPELINE_ITEM);

      // the compaction processing
      try {
        // Phase I: create the compacted MutableCellSetSegment
        compactSegments(result);

        // Phase II: swap the old compaction pipeline
        if (!isInterrupted.get()) {
          pipeline.swap(versionedList, result);
          // update the wal so it can be truncated and not get too long
          updateLowestUnflushedSequenceIdInWal(true); // only if greater
        }
      } catch (Exception e) {
        LOG.debug("Interrupting the MemStore in-memory compaction for store " + getFamilyName());
        Thread.currentThread().interrupt();
        return;
      } finally {
        releaseResources();
        inMemoryFlushInProgress.set(false);
      }

    }

    /**
     * Creates the scanner for compacting the pipeline.
     *
     * @return the scanner
     */
    private StoreScanner createScanner(Store store) throws IOException {

      Scan scan = new Scan();
      scan.setMaxVersions();  //Get all available versions

      StoreScanner internalScanner =
          new StoreScanner(store, store.getScanInfo(), scan, Collections.singletonList(scanner),
              ScanType.COMPACT_RETAIN_DELETES, smallestReadPoint, HConstants.OLDEST_TIMESTAMP);

      return internalScanner;
    }

    /**
     * Updates the given single StoreSegment using the internal store scanner,
     * who in turn uses ScanQueryMatcher
     */
    private void compactSegments(StoreSegment result) throws IOException {

      List<Cell> kvs = new ArrayList<Cell>();
      // get the limit to the size of the groups to be returned by compactingScanner
      int compactionKVMax = getConfiguration().getInt(
          HConstants.COMPACTION_KV_MAX,
          HConstants.COMPACTION_KV_MAX_DEFAULT);

      ScannerContext scannerContext =
          ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

      boolean hasMore;
      do {
        hasMore = compactingScanner.next(kvs, scannerContext);
        if (!kvs.isEmpty()) {
          for (Cell c : kvs) {
            // The scanner is doing all the elimination logic
            // now we just copy it to the new segment
            KeyValue kv = KeyValueUtil.ensureKeyValue(c);
            Cell newKV = result.maybeCloneWithAllocator(kv);
            result.add(newKV);

          }
          kvs.clear();
        }
      } while (hasMore && (!isInterrupted.get()));
    }

  }

  boolean isMemStoreFlushingInMemory() {
    return inMemoryFlushInProgress.get();
  }

  void disableCompaction() {
    allowCompaction.set(false);
  }

  void enableCompaction() {
    allowCompaction.set(true);
  }

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   *             first.
   * @return Next row or null if none found.
   */
  Cell getNextRow(final Cell cell) {
    Cell lowest = null;
    LinkedList<StoreSegment> segments = getListOfSegments();
    for (StoreSegment segment : segments) {
      if (lowest == null) {
        lowest = getNextRow(cell, segment.getCellSet());
      } else {
        lowest = getLowest(lowest, getNextRow(cell, segment.getCellSet()));
      }
    }
    return lowest;
  }

  // debug method
  private void debug() {
    String msg = "active size="+getActive().getSize();
    msg += " threshold="+0.9*flushSizeLowerBound;
    msg += " allow compaction is "+ (allowCompaction.get() ? "true" : "false");
    msg += " inMemoryFlushInProgress is "+ (inMemoryFlushInProgress.get() ? "true" : "false");
    LOG.debug(msg);
  }
}
