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
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is an abstraction of a cell set bucket maintained in a memstore, e.g., the active
 * cell set or a snapshot of it.
 * It mainly encapsulates the kv-set and its respective memory allocation buffers (MSLAB).
 * This class facilitates the management of the compaction pipeline and the shifts of these buckets
 * from active set to snapshot set in the default implementation.
 */
@InterfaceAudience.Private
final class MemStoreSegment {

  static final String USEMSLAB_KEY = "hbase.hregion.memstore.mslab.enabled";
  static final boolean USEMSLAB_DEFAULT = true;
  static final String MSLAB_CLASS_NAME = "hbase.regionserver.mslab.class";

  private volatile CellSet cellSet;
  private volatile MemStoreLAB memStoreLAB;
  private final CellComparator comparator;
  private TimeRangeTracker timeRangeTracker;
  private final AtomicLong size;
  private volatile boolean tagsPresent;

  // private c-tors. Instantiate objects only using factory
  private MemStoreSegment(CellSet cellSet, MemStoreLAB memStoreLAB, long size,
      CellComparator comparator) {
    this.cellSet = cellSet;
    this.memStoreLAB = memStoreLAB;
    this.comparator = comparator;
    this.timeRangeTracker = new TimeRangeTracker();
    this.size = new AtomicLong(size);
    this.tagsPresent = false;
  }

  private MemStoreSegment(CellSet cellSet, long size, CellComparator comparator) {
    this(cellSet, null, size, comparator);
  }

  public MemStoreSegmentScanner getScanner(long readPoint) {
    return new MemStoreSegmentScanner(this, readPoint);
  }

  public boolean isEmpty() {
    return getCellSet().isEmpty();
  }

  public int getCellsCount() {
    return getCellSet().size();
  }

  public long add(Cell e) {
    boolean succ = getCellSet().add(e);
    long s = AbstractMemStore.heapSizeChange(e, succ);
    updateMetaInfo(e, s);
    // In no tags case this NoTagsKeyValue.getTagsLength() is a cheap call.
    // When we use ACL CP or Visibility CP which deals with Tags during
    // mutation, the TagRewriteCell.getTagsLength() is a cheaper call. We do not
    // parse the byte[] to identify the tags length.
    if(e.getTagsLength() > 0) {
      tagsPresent = true;
    }
    return s;
  }

  public boolean remove(Cell e) {
    return getCellSet().remove(e);
  }

  public Cell get(Cell cell) {
    return getCellSet().get(cell);
  }

  public Cell last() {
    return getCellSet().last();
  }

  public Iterator<Cell> iterator() {
    return getCellSet().iterator();
  }

  public SortedSet<Cell> headSet(KeyValue firstKeyOnRow) {
    return getCellSet().headSet(firstKeyOnRow);
  }

  public SortedSet<Cell> tailSet(Cell firstCell) {
    return getCellSet().tailSet(firstCell);
  }

  public void close() {
    MemStoreLAB mslab = getMemStoreLAB();
    if(mslab != null) {
      mslab.close();
    }
    // do not set MSLab to null as scanners may still be reading the data here and need to decrease
    // the counter when they finish
  }

  public Cell maybeCloneWithAllocator(Cell cell) {
    if (getMemStoreLAB() == null) {
      return cell;
    }

    int len = KeyValueUtil.length(cell);
    ByteRange alloc = getMemStoreLAB().allocateBytes(len);
    if (alloc == null) {
      // The allocation was too large, allocator decided
      // not to do anything with it.
      return cell;
    }
    assert alloc.getBytes() != null;
    KeyValueUtil.appendToByteArray(cell, alloc.getBytes(), alloc.getOffset());
    KeyValue newKv = new KeyValue(alloc.getBytes(), alloc.getOffset(), len);
    newKv.setSequenceId(cell.getSequenceId());
    return newKv;
  }

  public void incScannerCount() {
    if(getMemStoreLAB() != null) {
      getMemStoreLAB().incScannerCount();
    }
  }

  public void decScannerCount() {
    if(getMemStoreLAB() != null) {
      getMemStoreLAB().decScannerCount();
    }
  }

  public long rollback(Cell cell) {
    Cell found = get(cell);
    if (found != null && found.getSequenceId() == cell.getSequenceId()) {
      long sz = AbstractMemStore.heapSizeChange(cell, true);
      remove(cell);
      size.addAndGet(-sz);
      return sz;
    }
    return 0;
  }

  public void updateMetaInfo(Cell toAdd, long s) {
    getTimeRangeTracker().includeTimestamp(toAdd);
    size.addAndGet(s);
  }

  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return (getTimeRangeTracker().includesTimeRange(scan.getTimeRange())
        && (getTimeRangeTracker().getMaximumTimestamp() >=
        oldestUnexpiredTS));
  }

  /*
 * @param set
 * @param state Accumulates deletes and candidates.
 */
//  public void getRowKeyAtOrBefore(final GetClosestRowBeforeTracker state) {
//    if (isEmpty()) {
//      return;
//    }
//    if (!walkForwardInSingleRow(state.getTargetKey(), state)) {
//      // Found nothing in row.  Try backing up.
//      getRowKeyBefore(state);
//    }
//  }

  // methods for cell set scanner
  public int compare(Cell left, Cell right) {
    return getComparator().compare(left, right);
  }

  public int compareRows(Cell left, Cell right) {
    return getComparator().compareRows(left, right);
  }

  public long getMinTimestamp() {
    return this.timeRangeTracker.getMinimumTimestamp();
  }

  public void setSize(long size) {
    this.size.set(size);
  }

  public CellSet getCellSet() {
    return cellSet;
  }

  public TimeRangeTracker getTimeRangeTracker() {
    return timeRangeTracker;
  }

  public long getSize() {
    return size.get();
  }

  public void incSize(long delta) {
    size.addAndGet(delta);
  }

  public boolean isTagsPresent() {
    return tagsPresent;
  }

  private MemStoreLAB getMemStoreLAB() {
    return memStoreLAB;
  }

  private CellComparator getComparator() {
    return comparator;
  }

  // methods for tests
  Cell first() {
    return this.getCellSet().first();
  }

  /*
   * Walk forward in a row from <code>firstOnRow</code>.  Presumption is that
   * we have been passed the first possible key on a row.  As we walk forward
   * we accumulate deletes until we hit a candidate on the row at which point
   * we return.
   * @param set
   * @param firstOnRow First possible key on this row.
   * @param state
   * @return True if we found a candidate walking this row.
   */
//  private boolean walkForwardInSingleRow(final Cell firstOnRow,
// final GetClosestRowBeforeTracker state) {
//    boolean foundCandidate = false;
//    SortedSet<Cell> tail = getCellSet().tailSet(firstOnRow);
//    if (tail.isEmpty()) return foundCandidate;
//    for (Iterator<Cell> i = tail.iterator(); i.hasNext();) {
//      Cell kv = i.next();
//      // Did we go beyond the target row? If so break.
//      if (state.isTooFar(kv, firstOnRow)) break;
//      if (state.isExpired(kv)) {
//        i.remove();
//        continue;
//      }
//      // If we added something, this row is a contender. break.
//      if (state.handle(kv)) {
//        foundCandidate = true;
//        break;
//      }
//    }
//    return foundCandidate;
//  }

  /*
   * Walk backwards through the passed set a row at a time until we run out of
   * set or until we get a candidate.
   * @param set
   * @param state
   */
//  private void getRowKeyBefore(final GetClosestRowBeforeTracker state) {
//    KeyValue firstOnRow = state.getTargetKey();
//    for (Cell p = memberOfPreviousRow(state, firstOnRow);
//         p != null; p = memberOfPreviousRow(state, firstOnRow)) {
//      // Make sure we don't fall out of our table.
//      if (!state.isTargetTable(p)) break;
//      // Stop looking if we've exited the better candidate range.
//      if (!state.isBetterCandidate(p)) break;
//      // Make into firstOnRow
//      firstOnRow = new KeyValue(p.getRowArray(), p.getRowOffset(), p.getRowLength(),
//          HConstants.LATEST_TIMESTAMP);
//      // If we find something, break;
//      if (walkForwardInSingleRow(firstOnRow, state)) break;
//    }
//  }

  /*
   * @param set Set to walk back in.  Pass a first in row or we'll return
   * same row (loop).
   * @param state Utility and context.
   * @param firstOnRow First item on the row after the one we want to find a
   * member in.
   * @return Null or member of row previous to <code>firstOnRow</code>
   */
//  private Cell memberOfPreviousRow(final GetClosestRowBeforeTracker state,
//      final KeyValue firstOnRow) {
//    NavigableSet<Cell> head = getCellSet().headSet(firstOnRow, false);
//    if (head.isEmpty()) return null;
//    for (Iterator<Cell> i = head.descendingIterator(); i.hasNext();) {
//      Cell found = i.next();
//      if (state.isExpired(found)) {
//        i.remove();
//        continue;
//      }
//      return found;
//    }
//    return null;
//  }

  /**
   * A singleton cell set manager factory.
   * Maps each cell set type to a specific implementation
   */
  static final class Factory {

    private Factory() {}
    private static Factory instance = new Factory();
    public static Factory instance() { return instance; }

    public MemStoreSegment createMemStoreSegment(CellSet.Type type, final Configuration conf,
        final CellComparator comparator, long size) {
      MemStoreLAB memStoreLAB = null;
      if (conf.getBoolean(USEMSLAB_KEY, USEMSLAB_DEFAULT)) {
        String className = conf.get(MSLAB_CLASS_NAME, HeapMemStoreLAB.class.getName());
        memStoreLAB = ReflectionUtils.instantiateWithCustomCtor(className,
            new Class[] { Configuration.class }, new Object[] { conf });
        //memStoreLAB = new MemStoreLAB(conf, MemStoreChunkPool.getPool(conf));
      }
      return createMemStoreSegment(type, comparator, memStoreLAB, size);
    }

    public MemStoreSegment createMemStoreSegment(CellSet.Type type,
        CellComparator comparator,
        long size) {
      return createMemStoreSegment(type, comparator, null, size);
    }

    public MemStoreSegment createMemStoreSegment(CellSet.Type type,
        CellComparator comparator,
        MemStoreLAB memStoreLAB, long size) {
      return generateMemStoreSegmentByType(type, comparator, memStoreLAB, size);
    }

    private MemStoreSegment generateMemStoreSegmentByType(CellSet.Type type,
        CellComparator comparator, MemStoreLAB memStoreLAB, long size) {
      CellSet set = new CellSet(type, comparator);
      return new MemStoreSegment(set, memStoreLAB, size, comparator);
    }

  }
}
