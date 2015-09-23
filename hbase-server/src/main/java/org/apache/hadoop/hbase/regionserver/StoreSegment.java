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
 * This is an abstraction of a segment maintained in a memstore, e.g., the active
 * cell set or its snapshot.
 *
 * This abstraction facilitates the management of the compaction pipeline and the shifts of these
 * segments from active set to snapshot set in the default implementation.
 */
public abstract class StoreSegment {

  private final TimeRangeTracker timeRangeTracker;
  protected volatile boolean tagsPresent;

  protected StoreSegment() {
    this.timeRangeTracker = new TimeRangeTracker();
    this.tagsPresent = false;
  }

  protected StoreSegment(StoreSegment segment) {
    this.timeRangeTracker = segment.getTimeRangeTracker();
    this.tagsPresent = segment.isTagsPresent();
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

  /**
   * Creates the scanner that is able to scan the concrete segment
   * @param readPoint
   * @return a scanner for the given read point
   */
  public abstract StoreSegmentScanner getScanner(long readPoint);

  /**
   * Returns whether the segment has any cells
   * @return whether the segment has any cells
   */
  public abstract boolean isEmpty();

  /**
   * Returns number of cells in segment
   * @return number of cells in segment
   */
  public abstract int getCellsCount();

  /**
   * Adds the given cell into the segment
   * @param cell
   * @return the change in the heap size
   */
  public abstract long add(Cell cell);

  /**
   * Removes the given cell from the segment
   * @param cell
   * @return the change in the heap size
   */
  public abstract long rollback(Cell cell);

  /**
   * Returns the first cell in the segment that has equal or greater key than the given cell
   * @param cell
   * @return the first cell in the segment that has equal or greater key than the given cell
   */
  public abstract Cell getFirstAfter(Cell cell);

  /**
   * Closing a segment before it is being discarded
   */
  public abstract void close();

  /**
   * If the segment has a memory allocator the cell is being cloned to this space, and returned;
   * otherwise the given cell is returned
   * @param cell
   * @return either the given cell or its clone
   */
  public abstract Cell maybeCloneWithAllocator(Cell cell);

  /**
   * Setting the heap size of the segment - used to account for different class overheads
   * @param size
   * @return this object
   */
  public abstract StoreSegment setSize(long size);

  /**
   * Returns the heap size of the segment
   * @return the heap size of the segment
   */
  public abstract long getSize();

  /**
   * Returns a set of all cells in the segment
   * @return a set of all cells in the segment
   */
  public abstract CellSet getCellSet();

  // Debug methods
  /**
   * Dumps all cells of the segment into the given log
   * @param log
   */
  public abstract void dump(Log log);

  @Override
  public String toString() {
    String res = "Store segment of type "+this.getClass().getName()+"; ";
    res += "isEmpty "+(isEmpty()?"yes":"no")+"; ";
    res += "cellCount "+getCellsCount()+"; ";
    res += "size "+getSize()+"; ";
    res += "Min ts "+getMinTimestamp()+"; ";
    return res;
  }

  protected TimeRangeTracker getTimeRangeTracker() {
    return timeRangeTracker;
  }

}
