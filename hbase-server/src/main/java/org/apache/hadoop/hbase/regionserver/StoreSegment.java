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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;

/**
 * This is an abstraction of a cell set segment maintained in a memstore, e.g., the active
 * cell set or a snapshot of it.
 *
 * This abstraction  facilitates the management of the compaction pipeline and the shifts of these
 * segments from active set to snapshot set in the default implementation.
 */
public abstract class StoreSegment {

  private final TimeRangeTracker timeRangeTracker;
  protected volatile boolean tagsPresent;

  protected StoreSegment() {
    this.timeRangeTracker = new TimeRangeTracker();
    this.tagsPresent = false;
  }

  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return (getTimeRangeTracker().includesTimeRange(scan.getTimeRange())
        && (getTimeRangeTracker().getMaximumTimestamp() >=
        oldestUnexpiredTS));
  }

  public long getMinTimestamp() {
    return getTimeRangeTracker().getMinimumTimestamp();
  }

  public boolean isTagsPresent() {
    return tagsPresent;
  }

  public abstract StoreSegmentScanner getScanner(long readPoint);
  public abstract boolean isEmpty();
  public abstract int getCellsCount();
  public abstract long add(Cell e);
  public abstract long rollback(Cell cell);
  public abstract Cell getFirstAfter(Cell cell);
  public abstract void close();
  public abstract Cell maybeCloneWithAllocator(Cell cell);
  public abstract StoreSegment setSize(long size);
  public abstract long getSize();
  public abstract CellSet getCellSet();


  public abstract void dump(Log log);

  protected TimeRangeTracker getTimeRangeTracker() {
    return timeRangeTracker;
  }

}
