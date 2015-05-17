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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

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

  private CompactionPipeline pipeline;
  private MemStoreCompactor compactor;
  private boolean forceFlush;

  private final static long ADDITIONAL_FIXED_OVERHEAD = ClassSize.align(
          (2 * ClassSize.REFERENCE) +
          (1 * Bytes.SIZEOF_BOOLEAN));

  private final static long DEEP_OVERHEAD_PER_ITEM = ClassSize.align(ClassSize.TIMERANGE_TRACKER +
      ClassSize.CELL_SKIPLIST_SET + ClassSize.CONCURRENT_SKIPLISTMAP);

  protected CompactedMemStore(Configuration conf,
      KeyValue.KVComparator c) throws IOException {
    super(conf, c);
    this.pipeline = new CompactionPipeline();
    this.compactor = new MemStoreCompactor(pipeline, c, 0, this);
    this.forceFlush = false;
  }

  @Override public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    LinkedList<CellSetMgr> list = getCellSetMgrList();
    for(CellSetMgr item : list) {
      if(item.shouldSeek(scan, oldestUnexpiredTS)) {
        return true;
      }
    }
    return false;
  }

  @Override protected long deepOverhead() {
    return DEEP_OVERHEAD + ADDITIONAL_FIXED_OVERHEAD +
        (pipeline.size() * DEEP_OVERHEAD_PER_ITEM);
  }

  @Override protected List<CellSetScanner> getListOfScanners(long readPt) throws IOException {
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
   */
  @Override public long size() {
    return 0;
  }

  /**
   * Creates a snapshot of the current memstore when force-flush flag is on,
   * otherwise ignores requests.
   * Snapshot must be cleared by call to
   * {@link #clearSnapshot(long)}.
   *
   * @return {@link MemStoreSnapshot}
   */
  @Override public MemStoreSnapshot snapshot() {
    return null;
  }

  /**
   * On flush, how much memory we will clear.
   * Flush will first clear out the data in snapshot if any (It will take a second flush
   * invocation to clear the current Cell set). If snapshot is empty, current
   * Cell set will be flushed.
   *
   * @return size of data that is going to be flushed
   */
  @Override public long getFlushableSize() {
    return 0;
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
    rollbackCellSet(cell);
  }

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately precedes it. The
   * target row key is set in state.
   *
   * @param state column/delete tracking state
   */
  @Override public void getRowKeyAtOrBefore(GetClosestRowBeforeTracker state) {
    getCellSet().getRowKeyAtOrBefore(state);
    pipeline.getRowKeyAtOrBefore(state);
    getSnapshot().getRowKeyAtOrBefore(state);
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
