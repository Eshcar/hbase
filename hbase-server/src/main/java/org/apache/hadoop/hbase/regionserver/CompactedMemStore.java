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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A memstore implementation which supports in-memory compaction.
 * A compaction pipeline is added between the active set and the snapshot data structures;
 * it consists of a list of kv-sets that are subject to compaction.
 * The semantics of the prepare-for-flush phase are changed: instead of shifting the current active
 * set to snapshot, the active set is pushed into the pipeline.
 * Like the snapshot, all pipeline components are read-only; updates only affect the active set.
 * To ensure this property we take advantage of the existing blocking mechanism -- the active set
 * is pushed to the pipeline while holding updatesLock in exclusive mode.
 * Periodically, a compaction is applied in the background to all pipeline components resulting
 * in a single read-only component. The “old” components are discarded when no scanner is reading
 * them.
 */
@InterfaceAudience.Private
public class CompactedMemStore extends AbstractMemStore {

  public final static long DEEP_OVERHEAD_PER_PIPELINE_ITEM = ClassSize.align(
      ClassSize.TIMERANGE_TRACKER +
          ClassSize.CELL_SKIPLIST_SET + ClassSize.CONCURRENT_SKIPLISTMAP);
  private static final Log LOG = LogFactory.getLog(CompactedMemStore.class);
  private HStore store;
  private CompactionPipeline pipeline;
  private MemStoreCompactor compactor;
  private boolean forceFlushToDisk;
  private NavigableMap<Long, Long> timestampToWALSeqId;

  public CompactedMemStore(Configuration conf, CellComparator c,
      HStore store) throws IOException {
    super(conf, c);
    this.store = store;
    this.pipeline = new CompactionPipeline(store.getHRegion());
    this.compactor = new MemStoreCompactor(this, pipeline, c, conf);
    this.forceFlushToDisk = false;
    this.timestampToWALSeqId = new TreeMap<>();
  }

  public static long getMemStoreSegmentSize(MemStoreSegment segment) {
    return segment.getSize() - DEEP_OVERHEAD_PER_PIPELINE_ITEM;
  }

  public static long getMemStoreSegmentListSize(LinkedList<MemStoreSegment> list) {
    long res = 0;
    for (MemStoreSegment segment : list) {
      res += getMemStoreSegmentSize(segment);
    }
    return res;
  }

  @Override
  protected List<MemStoreSegmentScanner> getListOfScanners(long readPt) throws IOException {
    LinkedList<MemStoreSegment> pipelineList = pipeline.getCellSetMgrList();
    LinkedList<MemStoreSegmentScanner> list = new LinkedList<MemStoreSegmentScanner>();
    list.add(getActive().getScanner(readPt));
    for (MemStoreSegment item : pipelineList) {
      list.add(item.getScanner(readPt));
    }
    list.add(getSnapshot().getScanner(readPt));
    // set sequence ids by decsending order
    Iterator<MemStoreSegmentScanner> iterator = list.descendingIterator();
    int seqId = 0;
    while (iterator.hasNext()) {
      iterator.next().setSequenceID(seqId);
      seqId++;
    }
    return list;
  }

  /**
   * @return Total memory occupied by this MemStore.
   * This is not thread safe and the memstore may be changed while computing its size.
   * It is the responsibility of the caller to make sure this doesn't happen.
   */
  @Override public long size() {
    long res = 0;
    for (MemStoreSegment item : getMemStoreSegmentList()) {
      res += item.getSize();
    }
    return res;
  }

  /**
   * Push the current active memstore bucket into the pipeline
   * and create a snapshot of the tail of current compaction pipeline
   * Snapshot must be cleared by call to {@link #clearSnapshot}.
   * {@link #clearSnapshot(long)}.
   * @param flushOpSeqId the sequence id that is attached to the flush operation in the wal
   *
   * @return {@link MemStoreSnapshot}
   */
  @Override public MemStoreSnapshot snapshot(long flushOpSeqId) {
    MemStoreSegment active = getActive();
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!getSnapshot().isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      LOG.info("FORCE FLUSH MODE: Pushing active set into compaction pipeline, " +
          "and pipeline tail into snapshot.");
      pushActiveToPipeline(active, flushOpSeqId, false);
      this.snapshotId = EnvironmentEdgeManager.currentTime();
      pushTailToSnapshot();
      resetForceFlush();
    }
    return new MemStoreSnapshot(this.snapshotId, getSnapshot(), getComparator());
  }

  @Override
  public void flushInMemory(long flushOpSeqId) {
    MemStoreSegment active = getActive();
    LOG.info("Pushing active set into compaction pipeline, and initiating compaction.");
    pushActiveToPipeline(active, flushOpSeqId, true);
    try {
      // Speculative compaction execution, may be interrupted if flush is forced while
      // compaction is in progress
      compactor.startCompact(store);
    } catch (IOException e) {
      LOG.error("Unable to run memstore compaction", e);
    }

  }

  @Override
  public void updateLowestUnflushedSequenceIdInWal(boolean onlyIfGreater) {
    long minTimestamp = pipeline.getMinTimestamp();
    Long seqId = getMaxSeqId(minTimestamp);
    if (seqId == null) return;
    byte[] encodedRegionName = getRegion().getRegionInfo().getEncodedNameAsBytes();
    byte[] familyName = getFamilyName();
    WAL wal = getRegion().getWAL();
    if (wal != null) {
      wal.updateStore(encodedRegionName, familyName, seqId, onlyIfGreater);
    }
  }

  private void pushActiveToPipeline(MemStoreSegment active, long flushOpSeqId,
      boolean needToUpdateRegionMemStoreSizeCounter) {
    if (!active.isEmpty()) {
      pipeline.pushHead(active);
      active.setSize(active.getSize() - deepOverhead() + DEEP_OVERHEAD_PER_PIPELINE_ITEM);
      long size = getMemStoreSegmentSize(active);
      resetCellSet();
      updateRegionAdditionalMemstoreSizeCounter(size); //push size into pipeline
      if (needToUpdateRegionMemStoreSizeCounter) {
        updateRegionMemStoreSizeCounter(-size);
      }
      Long now = EnvironmentEdgeManager.currentTime();
      timestampToWALSeqId.put(now, flushOpSeqId);
    }
  }

  private void pushTailToSnapshot() {
    MemStoreSegment tail = pipeline.pullTail();
    if (!tail.isEmpty()) {
      setSnapshot(tail);
      long size = getMemStoreSegmentSize(tail);
      setSnapshotSize(size);
      updateRegionAdditionalMemstoreSizeCounter(-size); //pull size out of pipeline
    }
  }

  private void updateRegionAdditionalMemstoreSizeCounter(long size) {
    if (getRegion() != null) {
      long globalMemstoreAdditionalSize = getRegion().addAndGetGlobalMemstoreAdditionalSize(size);
      // no need to update global memstore size as it is updated by the flusher
      LOG.debug(getRegion().getRegionInfo().getEncodedName() + " globalMemstoreAdditionalSize: " +
          globalMemstoreAdditionalSize);
    }
  }

  private void updateRegionMemStoreSizeCounter(long size) {
    if (getRegion() != null) {
      // need to update global memstore size when it is not accounted by the flusher
      long globalMemstoreSize = getRegion().addAndGetGlobalMemstoreSize(size);
      LOG.debug(getRegion().getRegionInfo().getEncodedName() + " globalMemstoreSize: " +
          globalMemstoreSize);
    }
  }

  /**
   * On flush, how much memory we will clear from the active cell set.
   *
   * @return size of data that is going to be flushed from active set
   */
  @Override
  public long getFlushableSize() {
    long snapshotSize = getSnapshot().getSize();
    return snapshotSize > 0 ? snapshotSize : keySize();
  }

  /**
   * Remove n key from the memstore. Only kvs that have the same key and the same memstoreTS are
   * removed. It is ok to not update timeRangeTracker in this call.
   *
   * @param cell
   */
  @Override public void rollback(Cell cell) {
    rollbackSnapshot(cell);
    pipeline.rollback(cell);
    rollbackActive(cell);
  }

  @Override
  public AbstractMemStore setForceFlushToDisk() {
    forceFlushToDisk = true;
    // stop compactor if currently working, to avoid possible conflict in pipeline
    compactor.stopCompact();
    return this;
  }

  @Override boolean isForceFlushToDisk() {
    return forceFlushToDisk;
  }

  @Override public boolean isCompactedMemStore() {
    return true;
  }

  @Override public boolean isMemStoreInCompaction() {
    return compactor.isInCompaction();
  }

  private CompactedMemStore resetForceFlush() {
    forceFlushToDisk = false;
    return this;
  }

  private LinkedList<MemStoreSegment> getMemStoreSegmentList() {
    LinkedList<MemStoreSegment> pipelineList = pipeline.getCellSetMgrList();
    LinkedList<MemStoreSegment> list = new LinkedList<MemStoreSegment>();
    list.add(getActive());
    list.addAll(pipelineList);
    list.add(getSnapshot());
    return list;
  }

  //methods for tests

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   *             first.
   * @return Next row or null if none found.
   */
  Cell getNextRow(final Cell cell) {
    Cell lowest = null;
    LinkedList<MemStoreSegment> segments = getMemStoreSegmentList();
    for (MemStoreSegment segment : segments) {
      if (lowest == null) {
        lowest = getNextRow(cell, segment.getCellSet());
      } else {
        lowest = getLowest(lowest, getNextRow(cell, segment.getCellSet()));
      }
    }
    return lowest;
  }

  void disableCompaction() {
    compactor.toggleCompaction(false);
  }

  void enableCompaction() {
    compactor.toggleCompaction(true);
  }

  public HRegion getRegion() {
    return store.getHRegion();
  }

  public byte[] getFamilyName() {
    return store.getFamily().getName();
  }

  /**
   * Returns the (maximal) sequence id that is associated with the maximal ts that is smaller than
   * the given ts, and removes all entries in the ts=>seqid map with timestamp smaller than
   * the given ts.
   *
   * @param minTimestamp
   * @return sequence id
   */
  public Long getMaxSeqId(long minTimestamp) {
    Long res = null;
    Long last = null;
    List<Long> tsToRemove = new LinkedList<Long>();
    for (Long ts : timestampToWALSeqId.keySet()) {
      if (ts >= minTimestamp) {
        break;
      }
      // else ts < min ts in memstore, therefore can use sequence id to truncate wal
      if (last != null) {
        tsToRemove.add(last);
      }
      last = ts;
    }
    if (last != null) {
      tsToRemove.add(last);
      res = timestampToWALSeqId.get(last);
    }
    for (Long ts : tsToRemove) {
      timestampToWALSeqId.remove(ts);
    }
    return res;
  }
}
