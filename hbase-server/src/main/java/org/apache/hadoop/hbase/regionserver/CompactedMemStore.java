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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A memstore implementation which supports in-memory compaction.
 * A compaction pipeline is added between the active set and the snapshot data structures;
 * it consists of a list of kv-sets that are subject to compaction.
 * The semantics of the prepare-for-flush phase are changed: instead of shifting the current active
 * set to snapshot, the active set is pushed into the pipeline.
 * Like the snapshot, all pipeline components are read-only; updates only affect the active set.
 * To ensure this property we take advantage of the existing blocking mechanism -- the active set
 * is pushed to the pipeline while holding updatesLock in exclusive mode.
 *
 * Periodically, a compaction is applied in the background to all pipeline components resulting
 * in a single read-only component. The “old” components are discarded when no scanner is reading
 * them.
 */
@InterfaceAudience.Private
public class CompactedMemStore extends AbstractMemStore {

  private static final Log LOG = LogFactory.getLog(CompactedMemStore.class);
  //private static final int CELLS_COUNT_MIN_THRESHOLD = 1000;
  //private static final int CELLS_SIZE_MIN_THRESHOLD = 4 * 1024 * 1024; //4MB

  private HRegion region;
  private CompactionPipeline pipeline;
  private MemStoreCompactor compactor;
  private boolean forceFlush;

  private final static long ADDITIONAL_FIXED_OVERHEAD = ClassSize.align(
          (2 * ClassSize.REFERENCE) +
          (1 * Bytes.SIZEOF_BOOLEAN));

  public final static long DEEP_OVERHEAD_PER_PIPELINE_ITEM = ClassSize.align(ClassSize
      .TIMERANGE_TRACKER +
      ClassSize.KEYVALUE_SKIPLIST_SET + ClassSize.CONCURRENT_SKIPLISTMAP);

  public static long getCellSetMgrSize(CellSetMgr cellSetMgr) {
    return cellSetMgr.getSize() - DEEP_OVERHEAD_PER_PIPELINE_ITEM;
  }

  public static long getCellSetMgrListSize(LinkedList<CellSetMgr> list) {
    long res = 0;
    for(CellSetMgr cellSetMgr : list) {
      res += getCellSetMgrSize(cellSetMgr);
    }
    return res;
  }

  /**
   * Default constructor. Used for tests.
   */
  CompactedMemStore() throws IOException {
    this(HBaseConfiguration.create(), KeyValue.COMPARATOR, null);
  }

  public CompactedMemStore(Configuration conf, KeyValue.KVComparator c,
      HRegion region) throws IOException {
    super(conf, c);
    this.region = region;
    this.pipeline = new CompactionPipeline(region);
    this.compactor = new MemStoreCompactor(this, pipeline, c, conf);
    this.forceFlush = false;
  }

  @Override
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    LinkedList<CellSetMgr> list = getCellSetMgrList();
    for(CellSetMgr item : list) {
      if(item.shouldSeek(scan, oldestUnexpiredTS)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the entire heap usage for this MemStore not including keys in the
   * snapshot.
   */
//  @Override
//  public long heapSize() {
//    long size = getCellSet().getSize();
//    for(CellSetMgr cellSetMgr : pipeline.getCellSetMgrList()) {
//      size += cellSetMgr.getSize();
//    }
//    return size;
//  }

  //  @Override
//  protected long deepOverhead() {
//    long pipelineSize = (pipeline == null ? 0 : pipeline.size());
//    return DEEP_OVERHEAD + ADDITIONAL_FIXED_OVERHEAD +
//        (pipelineSize * DEEP_OVERHEAD_PER_PIPELINE_ITEM);
//  }

  @Override
  protected List<CellSetScanner> getListOfScanners(long readPt) throws IOException {
    LinkedList<CellSetMgr> pipelineList = pipeline.getCellSetMgrList();
    List<CellSetScanner> list = new ArrayList<CellSetScanner>(2+pipelineList.size());
    list.add(getCellSet().getScanner(readPt));
    for(CellSetMgr item : pipelineList) {
      list.add(item.getScanner(readPt));
    }
    list.add(getSnapshot().getScanner(readPt));
    return list;
  }

  /**
   * @return Total memory occupied by this MemStore.
   * This is not thread safe and the memstore may be changed while computing its size.
   * It is the responsibility of the caller to make sure this doesn't happen.
   */
  @Override public long size() {
    long res = 0;
    for(CellSetMgr item : getCellSetMgrList()) {
      res += item.getSize();
    }
    return res;
  }

  /**
   * The semantics of the snapshot method are changed to do the following:
   * When force-flush flag is on, create a snapshot of the tail of current compaction pipeline
   * otherwise, push the current active memstore bucket into the pipeline.
   * Snapshot must be cleared by call to {@link #clearSnapshot}.
   */
  @Override
  public void snapshot() {
    CellSetMgr active = getCellSet();
    if(!forceFlush) {
      LOG.info("Snapshot called without forcing flush. ");
      LOG.info("Pushing active set into compaction pipeline, and initiating compaction.");
      pushActiveToPipeline(active);
      try {
        Map<byte[], Store> stores = region.getStores();
        Store store = stores.entrySet().iterator().next().getValue();
        compactor.doCompact(store);
        // compactor.doCompact(Long.MAX_VALUE);
      } catch (IOException e) {
        LOG.error("Unable to run memstore compaction", e);
      }
    } else { //**** FORCE FLUSH MODE ****//
      // If snapshot currently has entries, then flusher failed or didn't call
      // cleanup.  Log a warning.
      if (!getSnapshot().isEmpty()) {
        LOG.warn("Snapshot called again without clearing previous. " +
            "Doing nothing. Another ongoing flush or did we fail last attempt?");
      } else {
        LOG.info("FORCE FLUSH MODE: Pushing active set into compaction pipeline, " +
            "and pipeline tail into snapshot.");
        pushActiveToPipeline(active);
        this.snapshotId = EnvironmentEdgeManager.currentTimeMillis();
        pushTailToSnapshot();
        resetForceFlush();
      }
    }
//    return new MemStoreSnapshot(this.snapshotId, getSnapshot(), getComparator());

  }

  private void pushActiveToPipeline(CellSetMgr active) {
    if (!active.isEmpty()) {
      pipeline.pushHead(active);
      active.setSize(active.getSize() - deepOverhead() + DEEP_OVERHEAD_PER_PIPELINE_ITEM);
      long size = getCellSetMgrSize(active);
      resetCellSet();
      updateRegionCounters(size);
    }
  }

  private void pushTailToSnapshot() {
    CellSetMgr tail = pipeline.pullTail();
    setSnapshot(tail);
    long size = getCellSetMgrSize(tail);
    setSnapshotSize(size);
    updateRegionCounters(-size);
  }

  private void updateRegionCounters(long size) {
    if(getRegion() != null) {
      long globalMemstoreAdditionalSize = getRegion().addAndGetGlobalMemstoreAdditionalSize(size);
      // no need to update global memstore size as it is updated by the flusher
//      long globalMemstoreSize = getRegion().addAndGetGlobalMemstoreSize(-size);
      LOG.info(" globalMemstoreAdditionalSize: "+globalMemstoreAdditionalSize);
    }
  }

  /**
   * On flush, how much memory we will clear from the active cell set.
   *
   * @return size of data that is going to be flushed from active set
   */
  @Override
  public long getFlushableSize() {
//    long snapshotSize = getSnapshot().getSize();
//    if(forceFlush && snapshotSize == 0) {
//      if(!pipeline.isEmpty()) {
//        snapshotSize = pipeline.peekTail().getSize() - DEEP_OVERHEAD_PER_PIPELINE_ITEM;
//      } else {
//        snapshotSize = keySize();
//      }
//    }
//    return snapshotSize;
    return keySize();
  }

  /**
   * Remove n key from the memstore. Only kvs that have the same key and the same memstoreTS are
   * removed. It is ok to not update timeRangeTracker in this call.
   *
   * @param cell
   */
  @Override
  public void rollback(KeyValue cell) {
    rollbackSnapshot(cell);
    pipeline.rollback(cell);
    rollbackCellSet(cell);
  }

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately precedes it. The
   * target row key is set in state.
   *
   * @param state column/delete tracking state
   */
  @Override
  public void getRowKeyAtOrBefore(GetClosestRowBeforeTracker state) {
    getCellSet().getRowKeyAtOrBefore(state);
    pipeline.getRowKeyAtOrBefore(state);
    getSnapshot().getRowKeyAtOrBefore(state);
  }

  @Override
  public AbstractMemStore setForceFlush() {
    forceFlush = true;
    // stop compactor if currently working, to avoid possible conflict in pipeline
    compactor.stopCompact();
    return this;
  }

  @Override public boolean isMemstoreCompaction() {
    return compactor.isInCompaction();
  }

  private CompactedMemStore resetForceFlush() {
    forceFlush = false;
    return this;
  }

  private LinkedList<CellSetMgr> getCellSetMgrList() {
    LinkedList<CellSetMgr> pipelineList = pipeline.getCellSetMgrList();
    LinkedList<CellSetMgr> list = new LinkedList<CellSetMgr>();
    list.add(getCellSet());
    list.addAll(pipelineList);
    list.add(getSnapshot());
    return list;
  }

  //methods for tests

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   * first.
   * @return Next row or null if none found.
   */
  KeyValue getNextRow(final KeyValue cell) {
    KeyValue lowest = null;
    LinkedList<CellSetMgr> mgrs = getCellSetMgrList();
    for (CellSetMgr mgr : mgrs) {
      if (lowest==null) {
        lowest = getNextRow(cell, mgr.getCellSet());
      } else {
        lowest = getLowest(lowest, getNextRow(cell, mgr.getCellSet()));
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
    return region;
  }
}
