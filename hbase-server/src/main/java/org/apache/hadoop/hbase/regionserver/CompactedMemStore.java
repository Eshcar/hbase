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

import java.io.IOException;
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
  protected CompactedMemStore(Configuration conf,
      KeyValue.KVComparator c) {
    super(conf, c);
  }

  @Override public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return false;
  }

  @Override protected long deepOverhead() {
    return 0;
  }

  @Override protected List<CellSetScanner> getListOfScanners(long readPt) throws IOException {
    return null;
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
   * Remove n key from the memstore. Only kvs that have the same key and the same memstoreTS are
   * removed. It is ok to not update timeRangeTracker in this call.
   *
   * @param cell
   */
  @Override public void rollback(Cell cell) {

  }

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately precedes it. The
   * target row key is set in state.
   *
   * @param state column/delete tracking state
   */
  @Override public void getRowKeyAtOrBefore(GetClosestRowBeforeTracker state) {

  }
}
