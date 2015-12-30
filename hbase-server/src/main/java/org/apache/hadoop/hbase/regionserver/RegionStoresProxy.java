/*
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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.wal.WAL;

/**
 * RegionStoresProxy class is the interface through which memstore access services at the region level.
 * It also maintains additional data that is updated by memstores and can be queried by the region.
 * For example, when using alternative memory formats or due to compaction the memstore needs to
 * take occasional lock and update size counters at the region level.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionStoresProxy {

  private final HRegion region;

  // size of fluctuating memstore segments, e.g., in compaction pipeline
  private final AtomicLong memstoreFluctuatingSize = new AtomicLong(0);

  public RegionStoresProxy(HRegion region) {
    this.region = region;

  }

  public void blockUpdates() {
    this.region.blockUpdates();
  }

  public void unblockUpdates() {
    this.region.unblockUpdates();
  }

  public long addAndGetGlobalMemstoreSize(long size) {
    return this.region.addAndGetGlobalMemstoreSize(size);
  }

  public long addAndGetGlobalMemstoreFluctuatingSize(long size) {
    return this.memstoreFluctuatingSize.addAndGet(size);
  }

  public long getGlobalMemstoreActiveSize() {
    return this.region.getMemstoreSize() - memstoreFluctuatingSize.get();
  }

  public long getWalSequenceId() throws IOException {
    WAL wal = this.region.getWAL();
    return this.region.getNextSequenceId(wal);
  }

}
