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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An abstract class, which implements the behaviour shared by all concrete memstore instances.
 */
@InterfaceAudience.Private
public abstract class AbstractMemStore implements MemStore {

  private final Configuration conf;
  private final KeyValue.KVComparator comparator;

  // MemStore.  Use a CellSet rather than SkipListSet because of the
  // better semantics.  The Map will overwrite if passed a key it already had
  // whereas the Set will not add new Cell if key is same though value might be
  // different.  Value is not important -- just make sure always same
  // reference passed.
  private volatile CellSetMgr cellSet;

  // Used to track own heapSize
  private final AtomicLong size;

  private TimeRangeTracker timeRangeTracker;

  // Used to track when to flush
  private volatile long timeOfOldestEdit;

  protected AbstractMemStore(final Configuration conf, final KeyValue.KVComparator c) {
    this.conf = conf;
    this.comparator = c;
    this.size = new AtomicLong();
    resetCellSet();
  }

  protected void resetCellSet() {
    this.cellSet = CellSetMgr.Factory.instance().createCellSetMgr(
        CellSetMgr.Type.READ_WRITE, conf, comparator);
    this.timeRangeTracker = new TimeRangeTracker();
    // Reset heap to not include any keys
    this.size.set(deepOverhead());
    this.timeOfOldestEdit = Long.MAX_VALUE;
  }

  void dump(Log log) {
    for (Cell cell: this.cellSet.getCellSet()) {
      log.info(cell);
    }
  }
  /*
  * Calculate how the MemStore size has changed.  Includes overhead of the
  * backing Map.
  * @param cell
  * @param notpresent True if the cell was NOT present in the set.
  * @return Size
  */
  static long heapSizeChange(final Cell cell, final boolean notpresent) {
    return notpresent ? ClassSize.align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY
        + CellUtil.estimatedHeapSizeOf(cell)) : 0;
  }

  protected abstract long deepOverhead();

  /**
   * Write an update
   * @param cell
   * @return approximate size of the passed KV & newly added KV which maybe different than the
   *         passed-in KV
   */
  @Override
  public Pair<Long, Cell> add(Cell cell) {
    Cell toAdd = maybeCloneWithAllocator(cell);
    return new Pair<Long, Cell>(internalAdd(toAdd), toAdd);
  }

  /**
   * @return Oldest timestamp of all the Cells in the MemStore
   */
  @Override
  public long timeOfOldestEdit() {
    return timeOfOldestEdit;
  }


  /**
   * Remove n key from the memstore. Only kvs that have the same key and the same memstoreTS are
   * removed. It is ok to not update timeRangeTracker in this call.
   *
   * @param cell
   */
  @Override
  public void rollback(Cell cell) {
    // If the key is in the memstore, delete it. Update this.size.
    Cell found = this.cellSet.get(cell);
    if (found != null && found.getSequenceId() == cell.getSequenceId()) {
      removeFromCellSet(cell);
      long s = heapSizeChange(cell, true);
      this.size.addAndGet(-s);
    }
  }

  private boolean removeFromCellSet(Cell e) {
    boolean b = this.cellSet.remove(e);
    setOldestEditTimeToNow();
    return b;
  }

  /**
   * Write a delete
   * @param deleteCell
   * @return approximate size of the passed key and value.
   */
  @Override
  public long delete(Cell deleteCell) {
    long s = 0;
    Cell toAdd = maybeCloneWithAllocator(deleteCell);
    s += heapSizeChange(toAdd, addToCellSet(toAdd));
    timeRangeTracker.includeTimestamp(toAdd);
    this.size.addAndGet(s);
    return s;
  }

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately precedes it. The
   * target row key is set in state.
   *
   * @param state column/delete tracking state
   */
  @Override
  public void getRowKeyAtOrBefore(GetClosestRowBeforeTracker state) {
    getRowKeyAtOrBefore(cellSet.getCellSet(), state);
  }

  /*
   * @param set
   * @param state Accumulates deletes and candidates.
   */
  protected void getRowKeyAtOrBefore(final NavigableSet<Cell> set,
      final GetClosestRowBeforeTracker state) {
    if (set.isEmpty()) {
      return;
    }
    if (!walkForwardInSingleRow(set, state.getTargetKey(), state)) {
      // Found nothing in row.  Try backing up.
      getRowKeyBefore(set, state);
    }
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
  private boolean walkForwardInSingleRow(final SortedSet<Cell> set,
      final Cell firstOnRow, final GetClosestRowBeforeTracker state) {
    boolean foundCandidate = false;
    SortedSet<Cell> tail = set.tailSet(firstOnRow);
    if (tail.isEmpty()) return foundCandidate;
    for (Iterator<Cell> i = tail.iterator(); i.hasNext();) {
      Cell kv = i.next();
      // Did we go beyond the target row? If so break.
      if (state.isTooFar(kv, firstOnRow)) break;
      if (state.isExpired(kv)) {
        i.remove();
        continue;
      }
      // If we added something, this row is a contender. break.
      if (state.handle(kv)) {
        foundCandidate = true;
        break;
      }
    }
    return foundCandidate;
  }

  /*
   * Walk backwards through the passed set a row at a time until we run out of
   * set or until we get a candidate.
   * @param set
   * @param state
   */
  private void getRowKeyBefore(NavigableSet<Cell> set,
      final GetClosestRowBeforeTracker state) {
    Cell firstOnRow = state.getTargetKey();
    for (Member p = memberOfPreviousRow(set, state, firstOnRow);
         p != null; p = memberOfPreviousRow(p.set, state, firstOnRow)) {
      // Make sure we don't fall out of our table.
      if (!state.isTargetTable(p.cell)) break;
      // Stop looking if we've exited the better candidate range.
      if (!state.isBetterCandidate(p.cell)) break;
      // Make into firstOnRow
      firstOnRow = new KeyValue(p.cell.getRowArray(), p.cell.getRowOffset(), p.cell.getRowLength(),
          HConstants.LATEST_TIMESTAMP);
      // If we find something, break;
      if (walkForwardInSingleRow(p.set, firstOnRow, state)) break;
    }
  }

  /*
  * Immutable data structure to hold member found in set and the set it was
  * found in. Include set because it is carrying context.
  */
  private static class Member {
    final Cell cell;
    final NavigableSet<Cell> set;
    Member(final NavigableSet<Cell> s, final Cell kv) {
      this.cell = kv;
      this.set = s;
    }
  }

  /*
   * @param set Set to walk back in.  Pass a first in row or we'll return
   * same row (loop).
   * @param state Utility and context.
   * @param firstOnRow First item on the row after the one we want to find a
   * member in.
   * @return Null or member of row previous to <code>firstOnRow</code>
   */
  private Member memberOfPreviousRow(NavigableSet<Cell> set,
      final GetClosestRowBeforeTracker state, final Cell firstOnRow) {
    NavigableSet<Cell> head = set.headSet(firstOnRow, false);
    if (head.isEmpty()) return null;
    for (Iterator<Cell> i = head.descendingIterator(); i.hasNext();) {
      Cell found = i.next();
      if (state.isExpired(found)) {
        i.remove();
        continue;
      }
      return new Member(head, found);
    }
    return null;
  }

  /**
   * Update or insert the specified KeyValues.
   * <p>
   * For each KeyValue, insert into MemStore.  This will atomically upsert the
   * value for that row/family/qualifier.  If a KeyValue did already exist,
   * it will then be removed.
   * <p>
   * Currently the memstoreTS is kept at 0 so as each insert happens, it will
   * be immediately visible.  May want to change this so it is atomic across
   * all KeyValues.
   * <p>
   * This is called under row lock, so Get operations will still see updates
   * atomically.  Scans will only see each KeyValue update as atomic.
   *
   * @param cells
   * @param readpoint readpoint below which we can safely remove duplicate KVs
   * @return change in memstore size
   */
  @Override
  public long upsert(Iterable<Cell> cells, long readpoint) {
    long size = 0;
    for (Cell cell : cells) {
      size += upsert(cell, readpoint);
    }
    return size;
  }

  /**
   * Inserts the specified KeyValue into MemStore and deletes any existing
   * versions of the same row/family/qualifier as the specified KeyValue.
   * <p>
   * First, the specified KeyValue is inserted into the Memstore.
   * <p>
   * If there are any existing KeyValues in this MemStore with the same row,
   * family, and qualifier, they are removed.
   * <p>
   * Callers must hold the read lock.
   *
   * @param cell
   * @return change in size of MemStore
   */
  private long upsert(Cell cell, long readpoint) {
    // Add the Cell to the MemStore
    // Use the internalAdd method here since we (a) already have a lock
    // and (b) cannot safely use the MSLAB here without potentially
    // hitting OOME - see TestMemStore.testUpsertMSLAB for a
    // test that triggers the pathological case if we don't avoid MSLAB
    // here.
    long addedSize = internalAdd(cell);

    // Get the Cells for the row/family/qualifier regardless of timestamp.
    // For this case we want to clean up any other puts
    Cell firstCell = KeyValueUtil.createFirstOnRow(
        cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    SortedSet<Cell> ss = cellSet.tailSet(firstCell);
    Iterator<Cell> it = ss.iterator();
    // versions visible to oldest scanner
    int versionsVisible = 0;
    while ( it.hasNext() ) {
      Cell cur = it.next();

      if (cell == cur) {
        // ignore the one just put in
        continue;
      }
      // check that this is the row and column we are interested in, otherwise bail
      if (CellUtil.matchingRow(cell, cur) && CellUtil.matchingQualifier(cell, cur)) {
        // only remove Puts that concurrent scanners cannot possibly see
        if (cur.getTypeByte() == KeyValue.Type.Put.getCode() &&
            cur.getSequenceId() <= readpoint) {
          if (versionsVisible >= 1) {
            // if we get here we have seen at least one version visible to the oldest scanner,
            // which means we can prove that no scanner will see this version

            // false means there was a change, so give us the size.
            long delta = heapSizeChange(cur, true);
            addedSize -= delta;
            this.size.addAndGet(-delta);
            it.remove();
            setOldestEditTimeToNow();
          } else {
            versionsVisible++;
          }
        }
      } else {
        // past the row or column, done
        break;
      }
    }
    return addedSize;
  }

  /**
   * @param readPt
   * @return scanner over the memstore. This might include scanner over the snapshot when one is
   * present.
   */
  @Override public List<KeyValueScanner> getScanners(long readPt) throws IOException {
    return null;
  }

  /**
   * @return Total memory occupied by this MemStore.
   */
  @Override
  public long size() {
    return heapSize();
  }

  /**
   * Get the entire heap usage for this MemStore not including keys in the
   * snapshot.
   */
  @Override
  public long heapSize() {
    return size.get();
  }

  /**
   * Check if this cell set may contain the required keys
   * @param scan
   * @return False if the key definitely does not exist in this cell set
   */
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return (timeRangeTracker.includesTimeRange(scan.getTimeRange())
        && (timeRangeTracker.getMaximumTimestamp() >=
        oldestUnexpiredTS));
  }

  /*
   * @param a
   * @param b
   * @return Return lowest of a or b or null if both a and b are null
   */
  protected Cell getLowest(final Cell a, final Cell b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return comparator.compareRows(a, b) <= 0? a: b;
  }

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   * first.
   * @return Next row or null if none found.
   */
  Cell getNextRow(final Cell cell) {
    return getNextRow(cell, cellSet.getCellSet());
  }

  /*
   * @param key Find row that follows this one.  If null, return first.
   * @param map Set to look in for a row beyond <code>row</code>.
   * @return Next row or null if none found.  If one found, will be a new
   * KeyValue -- can be destroyed by subsequent calls to this method.
   */
  protected Cell getNextRow(final Cell key,
      final NavigableSet<Cell> set) {
    Cell result = null;
    SortedSet<Cell> tail = key == null? set: set.tailSet(key);
    // Iterate until we fall into the next row; i.e. move off current row
    for (Cell cell: tail) {
      if (comparator.compareRows(cell, key) <= 0)
        continue;
      // Note: Not suppressing deletes or expired cells.  Needs to be handled
      // by higher up functions.
      result = cell;
      break;
    }
    return result;
  }

  protected long updateColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long newValue, long now, Cell firstCell) {
    // logic here: the new ts MUST be at least 'now'. But it could be larger if necessary.
    // But the timestamp should also be max(now, mostRecentTsInMemstore)

    // so we cant add the new Cell w/o knowing what's there already, but we also
    // want to take this chance to delete some cells. So two loops (sad)

    SortedSet<Cell> ss = cellSet.tailSet(firstCell);
    for (Cell cell : ss) {
      // if this isnt the row we are interested in, then bail:
      if (!CellUtil.matchingColumn(cell, family, qualifier)
          || !CellUtil.matchingRow(cell, firstCell)) {
        break; // rows dont match, bail.
      }

      // if the qualifier matches and it's a put, just RM it out of the cellSet.
      if (cell.getTypeByte() == KeyValue.Type.Put.getCode() &&
          cell.getTimestamp() > now && CellUtil.matchingQualifier(firstCell, cell)) {
        now = cell.getTimestamp();
      }
    }

    // create or update (upsert) a new Cell with
    // 'now' and a 0 memstoreTS == immediately visible
    List<Cell> cells = new ArrayList<Cell>(1);
    cells.add(new KeyValue(row, family, qualifier, now, Bytes.toBytes(newValue)));
    return upsert(cells, 1L);
  }

  private Cell maybeCloneWithAllocator(Cell cell) {
    return cellSet.maybeCloneWithAllocator(cell);
  }

  /**
   * Internal version of add() that doesn't clone Cells with the
   * allocator, and doesn't take the lock.
   *
   * Callers should ensure they already have the read lock taken
   */
  private long internalAdd(final Cell toAdd) {
    long s = heapSizeChange(toAdd, addToCellSet(toAdd));
    timeRangeTracker.includeTimestamp(toAdd);
    this.size.addAndGet(s);
    return s;
  }

  private boolean addToCellSet(Cell e) {
    boolean b = cellSet.add(e);
    setOldestEditTimeToNow();
    return b;
  }

  private void setOldestEditTimeToNow() {
    if (timeOfOldestEdit == Long.MAX_VALUE) {
      timeOfOldestEdit = EnvironmentEdgeManager.currentTime();
    }
  }

  protected long keySize() {
    return heapSize() - deepOverhead();
  }

  protected KeyValue.KVComparator getComparator() {
    return comparator;
  }

  protected CellSetMgr getCellSet() {
    return cellSet;
  }

  protected TimeRangeTracker getTimeRangeTracker() {
    return timeRangeTracker;
  }

  public AtomicLong getSize() {
    return size;
  }
}
