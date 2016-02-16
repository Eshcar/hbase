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
 * Services a Store needs from a Region.
 * RegionServicesForStores class is the interface through which memstore access services at the region level.
 * For example, when using alternative memory formats or due to compaction the memstore needs to
 * take occasional lock and update size counters at the region level.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionServicesForStores {

  private final HRegion region;

  public RegionServicesForStores(HRegion region) {
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

  public long getWalSequenceId() throws IOException {
    WAL wal = this.region.getWAL();
    return this.region.getNextSequenceId(wal);
  }

}
