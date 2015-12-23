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

import org.apache.hadoop.hbase.wal.WAL;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class StoreServices {

  private final HRegion region;

  // size of fluctuating memstore segments, e.g., in compaction pipeline
  private final AtomicLong memstoreFluctuatingSize = new AtomicLong(0);

  public StoreServices(HRegion region) {
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

  public long addAndGetFluctuatingMemstoreSize(long size) {
    return this.memstoreFluctuatingSize.addAndGet(size);
  }

  public long getMemstoreSizeForFlushPolicy() {
    return this.region.getMemstoreSize() - getMemstoreFluctuatingSize();
  }

  public long getWalSequenceId() throws IOException {
    WAL wal = this.region.getWAL();
    return this.region.getNextSequenceId(wal);
  }

  private long getMemstoreFluctuatingSize() {
    return memstoreFluctuatingSize.get();
  }

}
