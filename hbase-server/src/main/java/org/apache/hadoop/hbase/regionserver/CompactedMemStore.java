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

/**
 * A memstore implementation which supports its in-memory compaction.
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
  private static final int CELLS_COUNT_MIN_THRESHOLD = 1000;
  private static final int CELLS_SIZE_MIN_THRESHOLD = 4 * 1024 * 1024; //4MB

  private CompactionPipeline pipeline;
  private MemStoreCompactor compactor;
  private boolean forceFlush;

  private final static long ADDITIONAL_FIXED_OVERHEAD = ClassSize.align(
          (2 * ClassSize.REFERENCE) +
          (1 * Bytes.SIZEOF_BOOLEAN));

  private final static long DEEP_OVERHEAD_PER_PIPELINE_ITEM = ClassSize.align(ClassSize
      .TIMERANGE_TRACKER +
      ClassSize.KEYVALUE_SKIPLIST_SET + ClassSize.CONCURRENT_SKIPLISTMAP);

  /**
   * Default constructor. Used for tests.
   */
  CompactedMemStore() throws IOException {
    this(HBaseConfiguration.create(), KeyValue.COMPARATOR);
  }

  public CompactedMemStore(Configuration conf, KeyValue.KVComparator c) throws IOException {
    super(conf, c);
    this.pipeline = new CompactionPipeline();
    this.compactor = new MemStoreCompactor(pipeline, c, 0, this);
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

  @Override
  protected long deepOverhead() {
    return DEEP_OVERHEAD + ADDITIONAL_FIXED_OVERHEAD +
        (pipeline.size() * DEEP_OVERHEAD_PER_PIPELINE_ITEM);
  }

  /**
   * @param readPt
   * @return scanner on memstore and snapshot in this order.
   */
  @Override public List<KeyValueScanner> getScanners(long readPt) throws IOException {
    return null;
  }

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
    if(!forceFlush) {
      LOG.info("Snapshot called without forcing flush. ");
      CellSetMgr active = getCellSet();
      if(active.getCellsCount() > CELLS_COUNT_MIN_THRESHOLD ||
          active.getSize() > CELLS_SIZE_MIN_THRESHOLD) {
        LOG.info("Pushing active set into compaction pipeline.");
        pipeline.pushHead(active);
        resetCellSet();
        compactor.doCompact();
      }
    } else { //**** FORCE FLUSH MODE ****//
      // If snapshot currently has entries, then flusher failed or didn't call
      // cleanup.  Log a warning.
      if (!getSnapshot().isEmpty()) {
        LOG.warn("Snapshot called again without clearing previous. " +
            "Doing nothing. Another ongoing flush or did we fail last attempt?");
      } else {
        this.snapshotId = EnvironmentEdgeManager.currentTimeMillis();
        CellSetMgr tail = pipeline.pullTail();
        setSnapshot(tail);
        setSnapshotSize(tail.getSize() - DEEP_OVERHEAD_PER_PIPELINE_ITEM);
        resetForceFlush();
      }
    }
//    return new MemStoreSnapshot(this.snapshotId, getSnapshot(), getComparator());

  }

  /**
   * On flush, how much memory we will clear.
   * If force flush flag is one flush will first clear out the data in snapshot if any.
   * If snapshot is empty, tail of pipeline will be flushed.
   *
   * @return size of data that is going to be flushed
   */
  @Override
  public long getFlushableSize() {
    long snapshotSize = getSnapshot().getSize();
    if(forceFlush && snapshotSize == 0) {
      snapshotSize = pipeline.peekTail().getSize();
    }
    return snapshotSize;
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

  public CompactedMemStore setForceFlush() {
    forceFlush = true;
    // stop compactor if currently working, to avoid possible conflict in pipeline
    compactor.stopCompact();
    return this;
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


}
