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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Pair;

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
public class CompactedMemStore implements MemStore {
  /**
   * Creates a snapshot of the current memstore. Snapshot must be cleared by call to
   * {@link #clearSnapshot(long)}.
   *
   * @return {@link MemStoreSnapshot}
   */
  @Override public MemStoreSnapshot snapshot() {
    return null;
  }

  /**
   * Clears the current snapshot of the Memstore.
   *
   * @param id
   * @throws org.apache.hadoop.hbase.regionserver.UnexpectedStateException
   * @see #snapshot()
   */
  @Override public void clearSnapshot(long id) throws UnexpectedStateException {

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
   * Return the size of the snapshot(s) if any
   *
   * @return size of the memstore snapshot
   */
  @Override public long getSnapshotSize() {
    return 0;
  }

  /**
   * Write an update
   *
   * @param cell
   * @return approximate size of the passed KV and the newly added KV which maybe different from the
   * passed in KV.
   */
  @Override public Pair<Long, Cell> add(Cell cell) {
    return null;
  }

  /**
   * @return Oldest timestamp of all the Cells in the MemStore
   */
  @Override public long timeOfOldestEdit() {
    return 0;
  }

  /**
   * Remove n key from the memstore. Only kvs that have the same key and the same memstoreTS are
   * removed. It is ok to not update timeRangeTracker in this call.
   *
   * @param cell
   */
  @Override public void rollback(Cell cell) {

  }

  /**
   * Write a delete
   *
   * @param deleteCell
   * @return approximate size of the passed key and value.
   */
  @Override public long delete(Cell deleteCell) {
    return 0;
  }

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately precedes it. The
   * target row key is set in state.
   *
   * @param state column/delete tracking state
   */
  @Override public void getRowKeyAtOrBefore(GetClosestRowBeforeTracker state) {

  }

  /**
   * Given the specs of a column, update it, first by inserting a new record,
   * then removing the old one.  Since there is only 1 KeyValue involved, the memstoreTS
   * will be set to 0, thus ensuring that they instantly appear to anyone. The underlying
   * store will ensure that the insert/delete each are atomic. A scanner/reader will either
   * get the new value, or the old value and all readers will eventually only see the new
   * value after the old was removed.
   *
   * @param row
   * @param family
   * @param qualifier
   * @param newValue
   * @param now
   * @return Timestamp
   */
  @Override public long updateColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long newValue, long now) {
    return 0;
  }

  /**
   * Update or insert the specified cells.
   * For each Cell, insert into MemStore. This will atomically upsert the value for that
   * row/family/qualifier. If a Cell did already exist, it will then be removed.
   * Currently the memstoreTS is kept at 0 so as each insert happens, it will be immediately
   * visible. May want to change this so it is atomic across all KeyValues.
   * This is called under row lock, so Get operations will still see updates atomically. Scans will
   * only see each KeyValue update as atomic.
   *
   * @param cells
   * @param readpoint readpoint below which we can safely remove duplicate Cells.
   * @return change in memstore size
   */
  @Override public long upsert(Iterable<Cell> cells, long readpoint) {
    return 0;
  }

  /**
   * @param readPt
   * @return scanner over the memstore. This might include scanner over the snapshot when one is
   * present.
   */
  @Override public List<KeyValueScanner> getScanners(long readPt) {
    return null;
  }

  /**
   * @return Total memory occupied by this MemStore.
   */
  @Override public long size() {
    return 0;
  }

  /**
   * @return Approximate 'exclusive deep size' of implementing object.  Includes
   * count of payload and hosting object sizings.
   */
  @Override public long heapSize() {
    return 0;
  }
}
