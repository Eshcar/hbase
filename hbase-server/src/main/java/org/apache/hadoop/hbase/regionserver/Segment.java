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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;

/**
 * This is an abstraction of a segment maintained in a memstore, e.g., the active
 * cell set or its snapshot.
 *
 * This abstraction facilitates the management of the compaction pipeline and the shifts of these
 * segments from active set to snapshot set in the default implementation.
 */
@InterfaceAudience.Private
public abstract class Segment {

  public final static long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
      + 5 * ClassSize.REFERENCE // cellSet, comparator, memStoreLAB, memStoreSizing,
                                // and timeRangeTracker
      + Bytes.SIZEOF_LONG // minSequenceId
      + Bytes.SIZEOF_BOOLEAN); // tagsPresent
  public final static long DEEP_OVERHEAD = FIXED_OVERHEAD + ClassSize.ATOMIC_REFERENCE
      + ClassSize.CELL_SET + 2 * ClassSize.ATOMIC_LONG;

  private AtomicReference<CellSet> cellSet= new AtomicReference<>();
  private final CellComparator comparator;
  protected long minSequenceId;
  private MemStoreLAB memStoreLAB;
  // Sum of sizes of all Cells added to this Segment. Cell's heapSize is considered. This is not
  // including the heap overhead of this class.
  protected final MemStoreSizing segmentSize;
//  protected final AtomicLong dataSize;
//  protected final AtomicLong heapSize;
  protected final TimeRangeTracker timeRangeTracker;
  protected volatile boolean tagsPresent;

  // Empty constructor to be used when Segment is used as interface,
  // and there is no need in true Segments state
  protected Segment(CellComparator comparator, TimeRangeTracker trt) {
    this.comparator = comparator;
    this.segmentSize = new MemStoreSizing();
//    this.dataSize = new AtomicLong(0);
//    this.heapSize = new AtomicLong(0);
    this.timeRangeTracker = trt;
  }

  // This constructor is used to create empty Segments.
  protected Segment(CellSet cellSet, CellComparator comparator, MemStoreLAB memStoreLAB, TimeRangeTracker trt) {
    this.cellSet.set(cellSet);
    this.comparator = comparator;
    this.minSequenceId = Long.MAX_VALUE;
    this.memStoreLAB = memStoreLAB;
    this.segmentSize = new MemStoreSizing();
//    this.dataSize = new AtomicLong(0);
//    this.heapSize = new AtomicLong(0);
    this.tagsPresent = false;
    this.timeRangeTracker = trt;
  }

  protected Segment(Segment segment) {
    this.cellSet.set(segment.getCellSet());
    this.comparator = segment.getComparator();
    this.minSequenceId = segment.getMinSequenceId();
    this.memStoreLAB = segment.getMemStoreLAB();
    this.segmentSize = new MemStoreSizing(segment.getMemStoreSizing());
//    this.dataSize = new AtomicLong(segment.keySize());
//    this.heapSize = new AtomicLong(segment.heapSize.get());
    this.tagsPresent = segment.isTagsPresent();
    this.timeRangeTracker = segment.getTimeRangeTracker();
  }

  /**
   * Creates the scanner for the given read point
   * @return a scanner for the given read point
   */
  protected KeyValueScanner getScanner(long readPoint) {
    return new SegmentScanner(this, readPoint);
  }

  /**
   * Creates the scanner for the given read point, and a specific order in a list
   * @return a scanner for the given read point
   */
  public KeyValueScanner getScanner(long readPoint, long order) {
    return new SegmentScanner(this, readPoint, order);
  }

  public List<KeyValueScanner> getScanners(long readPoint, long order) {
    return Collections.singletonList(new SegmentScanner(this, readPoint, order));
  }

  /**
   * @return whether the segment has any cells
   */
  public boolean isEmpty() {
    return getCellSet().isEmpty();
  }

  /**
   * @return number of cells in segment
   */
  public int getCellsCount() {
    return getCellSet().size();
  }

  /**
   * Closing a segment before it is being discarded
   */
  public void close() {
    if (this.memStoreLAB != null) {
      this.memStoreLAB.close();
    }
    // do not set MSLab to null as scanners may still be reading the data here and need to decrease
    // the counter when they finish
  }

  /**
   * If the segment has a memory allocator the cell is being cloned to this space, and returned;
   * otherwise the given cell is returned
   * @return either the given cell or its clone
   */
  public Cell maybeCloneWithAllocator(Cell cell) {
    if (this.memStoreLAB == null) {
      return cell;
    }

    Cell cellFromMslab = this.memStoreLAB.copyCellInto(cell);
    return (cellFromMslab != null) ? cellFromMslab : cell;
  }

  /**
   * Get cell length after serialized in {@link KeyValue}
   */
  @VisibleForTesting
  static int getCellLength(Cell cell) {
    return KeyValueUtil.length(cell);
  }

  public boolean shouldSeek(TimeRange tr, long oldestUnexpiredTS) {
    return !isEmpty()
        && (tr.isAllTime() || timeRangeTracker.includesTimeRange(tr))
        && timeRangeTracker.getMax() >= oldestUnexpiredTS;
  }

  public boolean isTagsPresent() {
    return tagsPresent;
  }

  public void incScannerCount() {
    if (this.memStoreLAB != null) {
      this.memStoreLAB.incScannerCount();
    }
  }

  public void decScannerCount() {
    if (this.memStoreLAB != null) {
      this.memStoreLAB.decScannerCount();
    }
  }

  /**
   * Setting the CellSet of the segment - used only for flat immutable segment for setting
   * immutable CellSet after its creation in immutable segment constructor
   * @return this object
   */

  protected Segment setCellSet(CellSet cellSetOld, CellSet cellSetNew) {
    this.cellSet.compareAndSet(cellSetOld, cellSetNew);
    return this;
  }

  public MemStoreSizing getMemStoreSizing() {
    return this.segmentSize;
  }

  /**
   * @return Sum of all cell's size.
   */
  public long keySize() {
    return this.segmentSize.getDataSize();
  }

  /**
   * @return The heap size of this segment.
   */
  public long heapSize() {
    return this.segmentSize.getHeapSize();
  }

  /**
   * @return The off-heap size of this segment.
   */
  public long offHeapSize() {
    return this.segmentSize.getOffHeapSize();
  }

  /**
   * Updates the size counters of the segment by the given delta
   */
  //TODO
  protected void incSize(long delta, long heapOverhead, long offHeapOverhead) {
    synchronized (this) {
      this.segmentSize.incMemStoreSize(delta, heapOverhead, offHeapOverhead);
    }
  }

  public long getMinSequenceId() {
    return minSequenceId;
  }

  public TimeRangeTracker getTimeRangeTracker() {
    return this.timeRangeTracker;
  }

  //*** Methods for SegmentsScanner
  public Cell last() {
    return getCellSet().last();
  }

  public Iterator<Cell> iterator() {
    return getCellSet().iterator();
  }

  public SortedSet<Cell> headSet(Cell firstKeyOnRow) {
    return getCellSet().headSet(firstKeyOnRow);
  }

  public int compare(Cell left, Cell right) {
    return getComparator().compare(left, right);
  }

  public int compareRows(Cell left, Cell right) {
    return getComparator().compareRows(left, right);
  }

  /**
   * @return a set of all cells in the segment
   */
  protected CellSet getCellSet() {
    return cellSet.get();
  }

  /**
   * Returns the Cell comparator used by this segment
   * @return the Cell comparator used by this segment
   */
  protected CellComparator getComparator() {
    return comparator;
  }

  protected void internalAdd(Cell cell, boolean mslabUsed, MemStoreSizing memstoreSizing) {
    boolean succ = getCellSet().add(cell);
    updateMetaInfo(cell, succ, mslabUsed, memstoreSizing);
  }

  protected void updateMetaInfo(Cell cellToAdd, boolean succ, boolean mslabUsed,
      MemStoreSizing memstoreSizing) {
    long cellSize = 0;
    // If there's already a same cell in the CellSet and we are using MSLAB, we must count in the
    // MSLAB allocation size as well, or else there will be memory leak (occupied heap size larger
    // than the counted number)
    if (succ || mslabUsed) {
      cellSize = getCellLength(cellToAdd);
    }
    long heapSize = heapSizeChange(cellToAdd, succ);
    long offHeapSize = offHeapSizeChange(cellToAdd, succ);
    incSize(cellSize, heapSize, offHeapSize);
    if (memstoreSizing != null) {
      memstoreSizing.incMemStoreSize(cellSize, heapSize, offHeapSize);
    }
    getTimeRangeTracker().includeTimestamp(cellToAdd);
    minSequenceId = Math.min(minSequenceId, cellToAdd.getSequenceId());
    // In no tags case this NoTagsKeyValue.getTagsLength() is a cheap call.
    // When we use ACL CP or Visibility CP which deals with Tags during
    // mutation, the TagRewriteCell.getTagsLength() is a cheaper call. We do not
    // parse the byte[] to identify the tags length.
    if (cellToAdd.getTagsLength() > 0) {
      tagsPresent = true;
    }
  }

  protected void updateMetaInfo(Cell cellToAdd, boolean succ, MemStoreSizing memstoreSizing) {
    updateMetaInfo(cellToAdd, succ, (getMemStoreLAB()!=null), memstoreSizing);
  }

  /**
   * @return The increase in heap size because of this cell addition. This includes this cell POJO's
   *         heap size itself and additional overhead because of addition on to CSLM.
   */
  protected long heapSizeChange(Cell cell, boolean succ) {
    long res = 0;
    if (succ) {
      boolean onHeap = true;
      MemStoreLAB memStoreLAB = getMemStoreLAB();
      if(memStoreLAB != null) {
        onHeap = memStoreLAB.isOnHeap();
      }
      res += indexEntryOnHeapSize(onHeap);
      if(onHeap) {
        res += PrivateCellUtil.estimatedSizeOfCell(cell);
      }
      res = ClassSize.align(res);
    }
    return res;
  }

  protected long offHeapSizeChange(Cell cell, boolean succ) {
    long res = 0;
    if (succ) {
      boolean offHeap = false;
      MemStoreLAB memStoreLAB = getMemStoreLAB();
      if(memStoreLAB != null) {
        offHeap = memStoreLAB.isOnHeap();
      }
      res += indexEntryOffHeapSize(offHeap);
      if(offHeap) {
        res += PrivateCellUtil.estimatedSizeOfCell(cell);
      }
      res = ClassSize.align(res);
    }
    return res;
  }

  protected long indexEntryOnHeapSize(boolean onHeap) {
    // in most cases index is allocated on-heap
    // override this method when it is not always the case, e.g., in CCM
    return indexEntrySize();
  }

  protected long indexEntryOffHeapSize(boolean offHeap) {
    // in most cases index is allocated on-heap
    // override this method when it is not always the case, e.g., in CCM
    return 0;
  }

  protected abstract long indexEntrySize();

  /**
   * Returns a subset of the segment cell set, which starts with the given cell
   * @param firstCell a cell in the segment
   * @return a subset of the segment cell set, which starts with the given cell
   */
  protected SortedSet<Cell> tailSet(Cell firstCell) {
    return getCellSet().tailSet(firstCell);
  }

  @VisibleForTesting
  MemStoreLAB getMemStoreLAB() {
    return memStoreLAB;
  }

  // Debug methods
  /**
   * Dumps all cells of the segment into the given log
   */
  void dump(Log log) {
    for (Cell cell: getCellSet()) {
      log.debug(cell);
    }
  }

  @Override
  public String toString() {
    String res = "Store segment of type "+this.getClass().getName()+"; ";
    res += "isEmpty "+(isEmpty()?"yes":"no")+"; ";
    res += "cellsCount "+getCellsCount()+"; ";
    res += "cellsSize "+keySize()+"; ";
    res += "totalHeapSize "+heapSize()+"; ";
    res += "Min ts " + timeRangeTracker.getMin() + "; ";
    res += "Max ts " + timeRangeTracker.getMax() + "; ";
    return res;
  }
}
