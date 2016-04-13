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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
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
          ClassSize.CELL_SET + ClassSize.CONCURRENT_SKIPLISTMAP);
  public final static long DEEP_OVERHEAD_PER_PIPELINE_FLAT_ARRAY_ITEM = ClassSize.align(
      ClassSize.TIMERANGE_TRACKER +
          ClassSize.CELL_SET + ClassSize.CELL_ARRAY_MAP);
  public final static double IN_MEMORY_FLUSH_THRESHOLD_FACTOR = 0.9;
  public final static double COMPACTION_TRIGGER_REMAIN_FACTOR = 1;
  public final static boolean COMPACTION_PRE_CHECK = false;

  static final String COMPACTING_MEMSTORE_TYPE_KEY = "hbase.hregion.compacting.memstore.type";
  static final int COMPACTING_MEMSTORE_TYPE_DEFAULT = 1;

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

  /**
   * Types of CompactingMemStore
   */
  public enum Type {
    COMPACT_TO_SKIPLIST_MAP,
    COMPACT_TO_ARRAY_MAP,
    COMPACT_TO_CHUNK_MAP;
  }

  private Type type = Type.COMPACT_TO_SKIPLIST_MAP;

  public CompactingMemStore(Configuration conf, CellComparator c,
      HStore store, RegionServicesForStores regionServices) throws IOException {
    super(conf, c);
    this.store = store;
    this.regionServices = regionServices;
    this.pipeline = new CompactionPipeline(getRegionServices());
    this.compactor = new MemStoreCompactor();
    initFlushSizeLowerBound(conf);
    int t = conf.getInt(COMPACTING_MEMSTORE_TYPE_KEY, COMPACTING_MEMSTORE_TYPE_DEFAULT);
    switch (t) {
    case 1: type = Type.COMPACT_TO_SKIPLIST_MAP;
      break;
    case 2: type = Type.COMPACT_TO_ARRAY_MAP;
      break;
    case 3: type = Type.COMPACT_TO_CHUNK_MAP;
      break;
    }
  }

  // C-tor for testing
  public CompactingMemStore(Configuration conf, CellComparator c,
      HStore store, RegionServicesForStores regionServices, Type type) throws IOException {
    this(conf,c,store,regionServices);
    this.type = type;
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
            + getRegionServices().getTableDesc().getTableName() + ":" + flushedSizeLowerBoundString
            + ". " + nfe + ", use global config(" + flushSizeLowerBound + ") instead");
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
      LOG.info("FLUSHING TO DISK: region "+ getRegionServices().getRegionInfo()
          .getRegionNameAsString() + "store: "+ getFamilyName());
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
      byte[] familyName = getFamilyNameInByte();
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
          + "store: "+ getFamilyName(), e);
    }
  }

  private String getFamilyName() {
    return Bytes.toString(getFamilyNameInByte());
  }

  private byte[] getFamilyNameInByte() {
    return store.getFamily().getName();
  }

  private ExecutorService getPool() {
    RegionServerServices rs = getRegionServices().getRegionServerServices();
    return (rs != null) ? rs.getExecutorService() : null;
  }

  private boolean shouldFlushInMemory() {
    if(getActive().getSize() > IN_MEMORY_FLUSH_THRESHOLD_FACTOR *flushSizeLowerBound) {
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
      super(getRegionServices().getRegionServerServices(),
          EventType.RS_IN_MEMORY_FLUSH_AND_COMPACTION);
    }

    @Override
    public void process() throws IOException {
      flushInMemory();
    }
  }

  /** ----------------------------------------------------------------------
   * The ongoing MemStore Compaction manager, dispatches a solo running compaction and interrupts
   * the compaction if requested. Prior to compaction the MemStoreCompactor evaluates
   * the compacting ratio and aborts the compaction if it is not worthy.
   * The MemStoreScanner is used to traverse the compaction pipeline. The MemStoreScanner
   * is included in internal store scanner, where all compaction logic is implemented.
   * Threads safety: It is assumed that the compaction pipeline is immutable,
   * therefore no special synchronization is required.
   */
  private class MemStoreCompactor {

    // a snapshot of the compaction pipeline segment list
    private VersionedSegmentsList versionedList;
    // a flag raised when compaction is requested to stop
    private final AtomicBoolean isInterrupted = new AtomicBoolean(false);
    // the limit to the size of the groups to be later provided to MemStoreCompactorIterator
    private final int compactionKVMax = getConfiguration().getInt(
        HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);

    /** ----------------------------------------------------------------------
     * The request to dispatch the compaction asynchronous task.
     * The method returns true if compaction was successfully dispatched, or false if there
     * is already an ongoing compaction (or pipeline is empty).
     */
    public boolean startCompact() throws IOException {
      if (pipeline.isEmpty()) return false;             // no compaction on empty pipeline
      // get a snapshot of the list of the segments from the pipeline,
      // this local copy of the list is marked with specific version
      versionedList = pipeline.getVersionedList();
      LOG.info(
          "Starting the MemStore in-memory compaction for store " + store.getColumnFamilyName());
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
      versionedList = null;
    }

    /*----------------------------------------------------------------------
    * The worker thread performs the compaction asynchronously.
    * The solo (per compactor) thread only reads the compaction pipeline.
    * There is at most one thread per memstore instance.
    */
    private void doCompact() {
      int cellsAfterComp = versionedList.getNumOfCells();
      try {
        // Phase I (optional): estimate the compaction expedience - CHECK COMPACTION
        if (COMPACTION_PRE_CHECK) {
          cellsAfterComp = countCellsForCompaction();

          if (!isInterrupted.get() && (cellsAfterComp
              > COMPACTION_TRIGGER_REMAIN_FACTOR * versionedList.getNumOfCells())) {
            // too much cells "survive" the possible compaction we do not want to compact!
            LOG.debug("Stopping the unworthy MemStore in-memory compaction for store "
                + getFamilyName());
            // Looking for Segment in the pipeline with SkipList index, to make it flat
            pipeline.flattenOneSegment(versionedList.getVersion());
            return;
          }
        }
        // Phase II: create the new compacted ImmutableSegment - START COMPACTION
        ImmutableSegment result = null;
        if (!isInterrupted.get()) {
          result = compact(cellsAfterComp);
        }
        // Phase III: swap the old compaction pipeline - END COMPACTION
        if (!isInterrupted.get()) {
          pipeline.swap(versionedList, result);
          // update the wal so it can be truncated and not get too long
          updateLowestUnflushedSequenceIdInWal(true); // only if greater
        }
      } catch (Exception e) {
        LOG.debug("Interrupting the MemStore in-memory compaction for store " + getFamilyName());
        Thread.currentThread().interrupt();
      } finally {
        releaseResources();
        inMemoryFlushInProgress.set(false);
      }
    }

    /**----------------------------------------------------------------------
     * The compaction is the creation of the relevant ImmutableSegment based on
     * the Compactor Itertor
     */
    private ImmutableSegment compact(int numOfCells)
        throws IOException {

      ImmutableSegment result = null;
      MemStoreCompactorIterator iterator =
          new MemStoreCompactorIterator(versionedList.getStoreSegments(), getComparator(),
              compactionKVMax, store);
      try {
        switch (type) {
        case COMPACT_TO_SKIPLIST_MAP:
          result = SegmentFactory.instance()
              .createImmutableSegment(getConfiguration(), getComparator(), iterator);
          break;
        case COMPACT_TO_ARRAY_MAP:
          result = SegmentFactory.instance()
              .createImmutableSegment(
                  getConfiguration(), getComparator(), iterator, numOfCells, true);
          break;
        case COMPACT_TO_CHUNK_MAP:
          result = SegmentFactory.instance()
              .createImmutableSegment(
                  getConfiguration(), getComparator(), iterator, numOfCells, false);
          break;
        default: throw new RuntimeException("Unknown type " + type); // sanity check
        }
      } finally {
        iterator.close();
      }

      return result;
    }

    /**----------------------------------------------------------------------
     * COUNT CELLS TO ESTIMATE THE EFFICIENCY OF THE FUTURE COMPACTION
     */
    private int countCellsForCompaction() throws IOException {

      int cnt = 0;
      MemStoreCompactorIterator iterator =
          new MemStoreCompactorIterator(versionedList.getStoreSegments(),getComparator(),
              compactionKVMax, store);

      try {
        while (iterator.next() != null) {
          cnt++;
        }
      } finally {
        iterator.close();
      }

      return cnt;
    }
  } // end of the MemStoreCompactor Class

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

  // debug method, also for testing
  public void debug() {
    String msg = "active size="+getActive().getSize();
    msg += " threshold="+ IN_MEMORY_FLUSH_THRESHOLD_FACTOR *flushSizeLowerBound;
    msg += " allow compaction is "+ (allowCompaction.get() ? "true" : "false");
    msg += " inMemoryFlushInProgress is "+ (inMemoryFlushInProgress.get() ? "true" : "false");
    LOG.debug(msg);
  }
}
