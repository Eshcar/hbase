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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

/**
 * A memstore implementation which supports in-memory compaction.
 * A compaction pipeline is added between the active set and the snapshot data structures;
 * it consists of a list of kv-sets that are subject to compaction.
 * Like the snapshot, all pipeline components are read-only; updates only affect the active set.
 * To ensure this property we take advantage of the existing blocking mechanism -- the active set
 * is pushed to the pipeline while holding the region's updatesLock in exclusive mode.
 * Periodically, a compaction is applied in the background to all pipeline components resulting
 * in a single read-only component. The ``old'' components are discarded when no scanner is reading
 * them.
 */
@InterfaceAudience.Private
public class CompactingMemStore extends AbstractMemStore {
  public final static long DEEP_OVERHEAD_PER_PIPELINE_ITEM = ClassSize.align(
      ClassSize.TIMERANGE_TRACKER +
          ClassSize.CELL_SKIPLIST_SET + ClassSize.CONCURRENT_SKIPLISTMAP);
  public final static double IN_MEMORY_FLUSH_THRESHOLD_FACTOR = 0.9;

  private static final Log LOG = LogFactory.getLog(CompactingMemStore.class);
  private HStore store;
  private RegionServicesForStores regionServices;
  private CompactionPipeline pipeline;
  private MemStoreCompactor compactor;
  // the threshold on active size for in-memory flush
  private long flushSizeLowerBound;
  private final AtomicBoolean inMemoryFlushInProgress = new AtomicBoolean(false);
  // A flag for tests only
  private final AtomicBoolean allowCompaction = new AtomicBoolean(true);

  public CompactingMemStore(Configuration conf, CellComparator c,
      HStore store, RegionServicesForStores regionServices) throws IOException {
    super(conf, c);
    this.store = store;
    this.regionServices = regionServices;
    this.pipeline = new CompactionPipeline(getRegionServices());
    this.compactor = new MemStoreCompactor();
    initFlushSizeLowerBound(conf);
  }

  private void initFlushSizeLowerBound(Configuration conf) {
    String flushedSizeLowerBoundString =
        getRegionServices().getTableDesc().getValue(FlushLargeStoresPolicy
            .HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND);
    if (flushedSizeLowerBoundString == null) {
      flushSizeLowerBound =
          conf.getLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
              FlushLargeStoresPolicy.DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN);
      if (LOG.isDebugEnabled()) {
        LOG.debug(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND
            + " is not specified, use global config(" + flushSizeLowerBound + ") instead");
      }
    } else {
      try {
        flushSizeLowerBound = Long.parseLong(flushedSizeLowerBoundString);
      } catch (NumberFormatException nfe) {
        flushSizeLowerBound =
            conf.getLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
                FlushLargeStoresPolicy.DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN);
        LOG.warn("Number format exception when parsing "
            + FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND + " for table "
            + getRegionServices().getTableDesc().getTableName() + ":" + flushedSizeLowerBoundString + ". "
            + nfe + ", use global config(" + flushSizeLowerBound + ") instead");
      }
    }
  }

  public static long getSegmentSize(Segment segment) {
    return segment.getSize() - DEEP_OVERHEAD_PER_PIPELINE_ITEM;
  }

  public static long getSegmentListSize(LinkedList<? extends Segment> list) {
    long res = 0;
    for (Segment segment : list) {
      res += getSegmentSize(segment);
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
    for (Segment item : getListOfSegments()) {
      res += item.getSize();
    }
    return res;
  }

  /**
   * This method is called when it is clear that the flush to disk is completed.
   * The store may do any post-flush actions at this point.
   * One example is to update the wal with sequence number that is known only at the store level.
   */
  @Override public void finalizeFlush() {
    updateLowestUnflushedSequenceIdInWal(false);
  }

  @Override public boolean isCompactingMemStore() {
    return true;
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
      LOG.info("FLUSHING TO DISK: region "+ getRegionServices().getRegionInfo().getRegionNameAsString()
          + "store: "+ Bytes.toString(getFamilyName()));
      stopCompact();
      pushActiveToPipeline(active);
      snapshotId = EnvironmentEdgeManager.currentTime();
      pushTailToSnapshot();
    }
    return new MemStoreSnapshot(snapshotId, getSnapshot());
  }

  /**
   * On flush, how much memory we will clear.
   * @return size of data that is going to be flushed
   */
  @Override public long getFlushableSize() {
    long snapshotSize = getSnapshot().getSize();
    if(snapshotSize == 0) {
      //if snapshot is empty the tail of the pipeline is flushed
      snapshotSize = pipeline.getTailSize();
    }
    return snapshotSize > 0 ? snapshotSize : keySize();
  }

  @Override
  public void updateLowestUnflushedSequenceIdInWal(boolean onlyIfGreater) {
    long minSequenceId = pipeline.getMinSequenceId();
    if(minSequenceId != Long.MAX_VALUE) {
      byte[] encodedRegionName = getRegionServices().getRegionInfo().getEncodedNameAsBytes();
      byte[] familyName = getFamilyName();
      WAL wal = getRegionServices().getWAL();
      if (wal != null) {
        wal.updateStore(encodedRegionName, familyName, minSequenceId, onlyIfGreater);
      }
    }
  }

  @Override
  public LinkedList<Segment> getListOfSegments() {
    LinkedList<Segment> pipelineList = pipeline.getStoreSegmentList();
    LinkedList<Segment> list = new LinkedList<Segment>();
    list.add(getActive());
    list.addAll(pipelineList);
    list.add(getSnapshot());
    return list;
  }

  @Override
  protected List<SegmentScanner> getListOfScanners(long readPt) throws IOException {
    LinkedList<Segment> pipelineList = pipeline.getStoreSegmentList();
    LinkedList<SegmentScanner> list = new LinkedList<SegmentScanner>();
    list.add(getActive().getSegmentScanner(readPt));
    for (Segment item : pipelineList) {
      list.add(item.getSegmentScanner(readPt));
    }
    list.add(getSnapshot().getSegmentScanner
        (readPt));
    // set sequence ids by decsending order
    Iterator<SegmentScanner> iterator = list.descendingIterator();
    int seqId = 0;
    while (iterator.hasNext()) {
      iterator.next().setSequenceID(seqId);
      seqId++;
    }
    return list;
  }

  /**
   * Check whether anything need to be done based on the current active set size.
   * The method is invoked upon every addition to the active set.
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
      if(pool != null) { // the pool can be null in some tests scenarios
        InMemoryFlushWorker worker = new InMemoryFlushWorker();
        LOG.info("Dispatching the MemStore in-memory flush for store "
            + store.getColumnFamilyName());
        pool.submit(worker);
        inMemoryFlushInProgress.set(true);
      }
    }
  }

  // internally used method, externally visible only for tests
  // when invoked directly from tests it must be verified that the caller doesn't hold updatesLock,
  // otherwise there is a deadlock
  void flushInMemory() throws IOException {
    // Phase I: Update the pipeline
    getRegionServices().blockUpdates();
    try {
      MutableSegment active = getActive();
      LOG.info("IN-MEMORY FLUSH: Pushing active segment into compaction pipeline, " +
          "and initiating compaction.");
      pushActiveToPipeline(active);
    } finally {
      getRegionServices().unblockUpdates();
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
          + getRegionServices().getRegionInfo().getRegionNameAsString()
          + "store: "+ Bytes.toString(getFamilyName()), e);
    }
  }

  private byte[] getFamilyName() {
    return store.getFamily().getName();
  }

  private ExecutorService getPool() {
    RegionServerServices rs = getRegionServices().getRegionServerServices();
    return (rs != null) ? rs.getExecutorService() : null;
  }

  private boolean shouldFlushInMemory() {
    if(getActive().getSize() > IN_MEMORY_FLUSH_THRESHOLD_FACTOR*flushSizeLowerBound) {
      // size above flush threshold
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

  private void pushActiveToPipeline(MutableSegment active) {
    if (!active.isEmpty()) {
      long delta = DEEP_OVERHEAD_PER_PIPELINE_ITEM - DEEP_OVERHEAD;
      active.setSize(active.getSize() + delta);
      pipeline.pushHead(active);
      resetCellSet();
    }
  }

  private void pushTailToSnapshot() {
    ImmutableSegment tail = pipeline.pullTail();
    if (!tail.isEmpty()) {
      setSnapshot(tail);
      long size = getSegmentSize(tail);
      setSnapshotSize(size);
    }
  }

  private RegionServicesForStores getRegionServices() {
    return regionServices;
  }



  /*----------------------------------------------------------------------
  * The in-memory-flusher thread performs the flush asynchronously.
  * There is at most one thread per memstore instance.
  * It takes the updatesLock exclusively, pushes active into the pipeline,
  * and compacts the pipeline.
  */
  private class InMemoryFlushWorker extends EventHandler {

    public InMemoryFlushWorker () {
      super(getRegionServices().getRegionServerServices(), EventType.RS_IN_MEMORY_FLUSH_AND_COMPACTION);
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

    // a static version of the segment list from the pipeline
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

      List<SegmentScanner> scanners = new ArrayList<SegmentScanner>();
      // get the list of segments from the pipeline
      versionedList = pipeline.getVersionedList();
      // the list is marked with specific version

      // create the list of scanners with maximally possible read point, meaning that
      // all KVs are going to be returned by the pipeline traversing
      for (Segment segment : versionedList.getStoreSegments()) {
        scanners.add(segment.getSegmentScanner(Long.MAX_VALUE));
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

      ImmutableSegment result = SegmentFactory.instance()  // create the scanner
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
     * Updates the given single Segment using the internal store scanner,
     * who in turn uses ScanQueryMatcher
     */
    private void compactSegments(Segment result) throws IOException {

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
            result.internalAdd(newKV);

          }
          kvs.clear();
        }
      } while (hasMore && (!isInterrupted.get()));
    }

  }

  //----------------------------------------------------------------------
  //methods for tests
  //----------------------------------------------------------------------
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
    LinkedList<Segment> segments = getListOfSegments();
    for (Segment segment : segments) {
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
    msg += " threshold="+IN_MEMORY_FLUSH_THRESHOLD_FACTOR*flushSizeLowerBound;
    msg += " allow compaction is "+ (allowCompaction.get() ? "true" : "false");
    msg += " inMemoryFlushInProgress is "+ (inMemoryFlushInProgress.get() ? "true" : "false");
    LOG.debug(msg);
  }
}
