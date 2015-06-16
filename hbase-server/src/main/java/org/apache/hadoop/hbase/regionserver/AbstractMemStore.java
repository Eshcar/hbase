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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;

/**
 * An abstract class, which implements the behaviour shared by all concrete memstore instances.
 */
@InterfaceAudience.Private
public abstract class AbstractMemStore implements HeapSize {

  private final Configuration conf;
  private final KeyValue.KVComparator comparator;

  // MemStore.  Use a CellSet rather than SkipListSet because of the
  // better semantics.  The Map will overwrite if passed a key it already had
  // whereas the Set will not add new Cell if key is same though value might be
  // different.  Value is not important -- just make sure always same
  // reference passed.
  volatile private CellSetMgr cellSet;
  // Snapshot of memstore.  Made for flusher.
  volatile private CellSetMgr snapshot;
  volatile long snapshotId;
  // Used to track when to flush
  volatile private long timeOfOldestEdit;

  public final static long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT +
          (4 * ClassSize.REFERENCE) +
          (2 * Bytes.SIZEOF_LONG));

  public final static long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
      2 * (ClassSize.ATOMIC_LONG + ClassSize.TIMERANGE_TRACKER +
      ClassSize.KEYVALUE_SKIPLIST_SET + ClassSize.CONCURRENT_SKIPLISTMAP));

  protected AbstractMemStore(final Configuration conf, final KeyValue.KVComparator c) {
    this.conf = conf;
    this.comparator = c;
    resetCellSet();
    this.snapshot = CellSetMgr.Factory.instance().createCellSetMgr(
        CellSetMgr.Type.EMPTY_SNAPSHOT, conf,c, 0);

  }

  protected void resetCellSet() {
    // Reset heap to not include any keys
    this.cellSet = CellSetMgr.Factory.instance().createCellSetMgr(
        CellSetMgr.Type.READ_WRITE, conf, comparator, deepOverhead());
    this.timeOfOldestEdit = Long.MAX_VALUE;
  }

  /*
  * Calculate how the MemStore size has changed.  Includes overhead of the
  * backing Map.
  * @param cell
  * @param notpresent True if the cell was NOT present in the set.
  * @return Size
  */
  static long heapSizeChange(final KeyValue cell, final boolean notpresent) {
    return notpresent ? ClassSize.align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY
        + cell.heapSize()) : 0;
  }
  /**
   * Creates a snapshot of the current memstore. Snapshot must be cleared by call to
   * {@link #clearSnapshot}.
   *
   */
  public abstract void snapshot();

  /**
   * On flush, how much memory we will clear.
   * Flush will first clear out the data in snapshot if any (It will take a second flush
   * invocation to clear the current Cell set). If snapshot is empty, current
   * Cell set will be flushed.
   *
   * @return size of data that is going to be flushed
   */
  public abstract long getFlushableSize();


  /**
   * Remove n key from the memstore. Only kvs that have the same key and the same memstoreTS are
   * removed. It is ok to not update timeRangeTracker in this call.
   * @param cell
   */
  public abstract void rollback(final KeyValue cell);

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately precedes it. The
   * target row key is set in state.
   * @param state column/delete tracking state
   */
  public abstract void getRowKeyAtOrBefore(final GetClosestRowBeforeTracker state);

  public abstract boolean shouldSeek(Scan scan, long oldestUnexpiredTS);

  /**
   * @return Total memory occupied by this MemStore.
   */
  public abstract long size();

  public abstract AbstractMemStore setForceFlush();

  protected abstract long deepOverhead();

  /**
   * Write an update
   * @param cell
   * @return approximate size of the passed KV & newly added KV which maybe different than the
   *         passed-in KV
   */
  public long add(KeyValue cell) {
    KeyValue toAdd = maybeCloneWithAllocator(cell);
    //return new Pair<Long, Cell>(internalAdd(toAdd), toAdd);
    return internalAdd(toAdd);
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
  public long upsert(Iterable<Cell> cells, long readpoint) {
    long size = 0;
    for (Cell cell : cells) {
      size += upsert(cell, readpoint);
    }
    return size;
  }

  /**
   * @return Oldest timestamp of all the Cells in the MemStore
   */
  public long timeOfOldestEdit() {
    return timeOfOldestEdit;
  }


  /**
   * Write a delete
   * @param deleteCell
   * @return approximate size of the passed key and value.
   */
  public long delete(KeyValue deleteCell) {
    long s = 0;
    KeyValue toAdd = maybeCloneWithAllocator(deleteCell);
    s += heapSizeChange(toAdd, addToCellSet(toAdd));
    cellSet.includeCell(toAdd, s);
    return s;
  }

  /**
   * The passed snapshot was successfully persisted; it can be let go.
   * @param ss the snapshot to clean out.
   * @throws UnexpectedException
   * @see #snapshot()
   */
  void clearSnapshot(final SortedSet<KeyValue> ss)
      throws UnexpectedException {

    if (this.snapshot.getCellSet() != ss) {
      throw new UnexpectedException("Current snapshot is " +
          this.snapshot.getCellSet() + ", was passed " + ss);
    }
    // OK. Passed in snapshot is same as current snapshot.  If not-empty,
    // create a new snapshot and let the old one go.
    CellSetMgr oldSnapshot = this.snapshot;
    if (!this.snapshot.isEmpty()) {
      this.snapshot = CellSetMgr.Factory.instance().createCellSetMgr(
          CellSetMgr.Type.EMPTY_SNAPSHOT, getComparator(), 0);
    }
    this.snapshotId = -1;
    oldSnapshot.close();
  }


  /**
   * Get the entire heap usage for this MemStore not including keys in the
   * snapshot.
   */
  @Override
  public long heapSize() {
    return getCellSet().getSize();
  }

  /**
   * @return scanner on memstore and snapshot in this order.
   */
  public List<KeyValueScanner> getScanners(long readPt) throws IOException {
    return Collections.<KeyValueScanner> singletonList(new MemStoreScanner(this, readPt));
  }

  public long getSnapshotSize() {
    return getSnapshot().getSize();
  }

  protected void rollbackSnapshot(KeyValue cell) {
    // If the key is in the snapshot, delete it. We should not update
    // this.size, because that tracks the size of only the memstore and
    // not the snapshot. The flush of this snapshot to disk has not
    // yet started because Store.flush() waits for all rwcc transactions to
    // commit before starting the flush to disk.
    snapshot.rollback(cell);
  }

  protected void rollbackCellSet(KeyValue cell) {
    // If the key is in the memstore, delete it. Update this.size.
    long sz = cellSet.rollback(cell);
    if (sz != 0) {
      setOldestEditTimeToNow();
    }
  }


  protected void dump(Log log) {
    for (Cell cell: this.cellSet.getCellSet()) {
      log.info(cell);
    }
    for (Cell cell: this.snapshot.getCellSet()) {
      log.info(cell);
    }
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
   * @param c
   * @return change in size of MemStore
   */
  private long upsert(Cell c, long readpoint) {
    // Add the Cell to the MemStore
    // Use the internalAdd method here since we (a) already have a lock
    // and (b) cannot safely use the MSLAB here without potentially
    // hitting OOME - see TestMemStore.testUpsertMSLAB for a
    // test that triggers the pathological case if we don't avoid MSLAB
    // here.
    KeyValue cell = KeyValueUtil.ensureKeyValue(c);
    long addedSize = internalAdd(cell);

    // Get the Cells for the row/family/qualifier regardless of timestamp.
    // For this case we want to clean up any other puts
    KeyValue firstCell = KeyValue.createFirstOnRow(
        cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    SortedSet<KeyValue> ss = cellSet.tailSet(firstCell);
    Iterator<KeyValue> it = ss.iterator();
    // versions visible to oldest scanner
    int versionsVisible = 0;
    while ( it.hasNext() ) {
      KeyValue cur = it.next();

      if (cell == cur) {
        // ignore the one just put in
        continue;
      }
      // check that this is the row and column we are interested in, otherwise bail
      if (CellUtil.matchingRow(cell, cur) && CellUtil.matchingQualifier(cell, cur)) {
        // only remove Puts that concurrent scanners cannot possibly see
        if (cur.getTypeByte() == KeyValue.Type.Put.getCode() &&
            cur.getMvccVersion() <= readpoint) {
          if (versionsVisible >= 1) {
            // if we get here we have seen at least one version visible to the oldest scanner,
            // which means we can prove that no scanner will see this version

            // false means there was a change, so give us the size.
            long delta = heapSizeChange(cur, true);
            addedSize -= delta;
            cellSet.incSize(-delta);
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

  /*
   * @param a
   * @param b
   * @return Return lowest of a or b or null if both a and b are null
   */
  protected KeyValue getLowest(final KeyValue a, final KeyValue b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return comparator.compareRows(a, b) <= 0? a: b;
  }

  /*
   * @param key Find row that follows this one.  If null, return first.
   * @param map Set to look in for a row beyond <code>row</code>.
   * @return Next row or null if none found.  If one found, will be a new
   * KeyValue -- can be destroyed by subsequent calls to this method.
   */
  protected KeyValue getNextRow(final KeyValue key,
      final NavigableSet<KeyValue> set) {
    KeyValue result = null;
    SortedSet<KeyValue> tail = key == null? set: set.tailSet(key);
    // Iterate until we fall into the next row; i.e. move off current row
    for (KeyValue cell: tail) {
      if (comparator.compareRows(cell, key) <= 0)
        continue;
      // Note: Not suppressing deletes or expired cells.  Needs to be handled
      // by higher up functions.
      result = cell;
      break;
    }
    return result;
  }

  /**
   * Only used by tests. TODO: Remove
   *
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
   * @return  Timestamp
   */
  public long updateColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long newValue, long now) {
    KeyValue firstCell = KeyValue.createFirstOnRow(row, family, qualifier);
    // Is there a Cell in 'snapshot' with the same TS? If so, upgrade the timestamp a bit.
    SortedSet<KeyValue> snSs = snapshot.tailSet(firstCell);
    if (!snSs.isEmpty()) {
      Cell snc = snSs.first();
      // is there a matching Cell in the snapshot?
      if (CellUtil.matchingRow(snc, firstCell) && CellUtil.matchingQualifier(snc, firstCell)) {
        if (snc.getTimestamp() == now) {
          // poop,
          now += 1;
        }
      }
    }
    // logic here: the new ts MUST be at least 'now'. But it could be larger if necessary.
    // But the timestamp should also be max(now, mostRecentTsInMemstore)

    // so we cant add the new Cell w/o knowing what's there already, but we also
    // want to take this chance to delete some cells. So two loops (sad)

    SortedSet<KeyValue> ss = cellSet.tailSet(firstCell);
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

  private KeyValue maybeCloneWithAllocator(KeyValue cell) {
    return cellSet.maybeCloneWithAllocator(cell);
  }

  /**
   * Internal version of add() that doesn't clone Cells with the
   * allocator, and doesn't take the lock.
   *
   * Callers should ensure they already have the read lock taken
   */
  private long internalAdd(final KeyValue toAdd) {
    boolean succ = addToCellSet(toAdd);
    long s = heapSizeChange(toAdd, succ);
    cellSet.includeCell(toAdd, s);
    return s;
  }

  private boolean addToCellSet(KeyValue e) {
    boolean b = cellSet.add(e);
    setOldestEditTimeToNow();
    return b;
  }

  private void setOldestEditTimeToNow() {
    if (timeOfOldestEdit == Long.MAX_VALUE) {
      timeOfOldestEdit = EnvironmentEdgeManager.currentTimeMillis();
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

  protected CellSetMgr getSnapshot() {
    return snapshot;
  }

  protected void setSnapshot(CellSetMgr snapshot) {
    this.snapshot = snapshot;
  }

  protected void setSnapshotSize(long snapshotSize) {
    getSnapshot().setSize(snapshotSize);
  }

  abstract protected List<CellSetScanner> getListOfScanners(long readPt) throws IOException;

}
