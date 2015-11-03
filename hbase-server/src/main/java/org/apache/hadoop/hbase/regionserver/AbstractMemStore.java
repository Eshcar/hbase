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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
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
public abstract class AbstractMemStore implements MemStore {

  private final Configuration conf;
  private final CellComparator comparator;

  // active segment absorbs write operations
  volatile private MutableSegment active;
  // Snapshot of memstore.  Made for flusher.
  volatile private ImmutableSegment snapshot;
  volatile long snapshotId;
  // Used to track when to flush
  volatile private long timeOfOldestEdit;

  public final static long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT +
          (4 * ClassSize.REFERENCE) +
          (2 * Bytes.SIZEOF_LONG));

  public final static long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
      2 * (ClassSize.ATOMIC_LONG + ClassSize.TIMERANGE_TRACKER +
      ClassSize.CELL_SKIPLIST_SET + ClassSize.CONCURRENT_SKIPLISTMAP));


  protected AbstractMemStore(final Configuration conf, final CellComparator c) {
    this.conf = conf;
    this.comparator = c;
    resetCellSet();
    this.snapshot = StoreSegmentFactory.instance().createImmutableSegment(conf, c, 0);

  }

  protected void resetCellSet() {
    // Reset heap to not include any keys
    this.active = StoreSegmentFactory.instance().createMutableSegment(
        conf, comparator, deepOverhead());
    this.timeOfOldestEdit = Long.MAX_VALUE;
  }

  /*
  * Calculate how the MemStore size has changed.  Includes overhead of the
  * backing Map.
  * @param cell
  * @param notpresent True if the cell was NOT present in the set.
  * @return change in size
  */
  static long heapSizeChange(final Cell cell, final boolean notpresent) {
    return notpresent ? ClassSize.align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY
        + CellUtil.estimatedHeapSizeOf(cell)) : 0;
  }

  /**
   * Sets the force flush to disk mode on.
   * @return this memstore
   */
  public abstract AbstractMemStore setForceFlushToDisk();

  /**
   * Returns true if the force flush to disk mode is set
   * @return true if the force flush to disk mode is set
   */
  abstract boolean isForceFlushToDisk();


  /**
   * Returns true if an in-memory compaction is in progress
   * @return true if an in-memory compaction is in progress
   */
  public abstract boolean isMemStoreInCompaction();

  /**
   * Flushes the active segment into a different (immutable) memory segments.
   * @param flushOpSeqId the sequence id used by the wal to mark this flush
   */
  public abstract void flushInMemory(long flushOpSeqId);

  /**
   * Updates the wal with the lowest sequence id (oldest entry) that is still in memory
   * @param onlyIfGreater a flag that marks whether to update the sequence id no matter what or
   *                      only if it is greater than the previous sequence id
   */
  public abstract void updateLowestUnflushedSequenceIdInWal(boolean onlyIfGreater);

  //method for tests
  /**
   * Returns true if the memstore supports in-memory compaction
   * @return true if the memstore supports in-memory compaction
   */
  public abstract boolean isCompactibleMemStore();

  boolean shouldFlushInMemory() {
    return !isForceFlushToDisk();
  }

  protected long deepOverhead() {
    return DEEP_OVERHEAD;
  }

  /**
   * Write an update
   * @param cell
   * @return approximate size of the passed cell & newly added cell which maybe different than the
   *         passed-in cell
   */
  @Override
  public long add(Cell cell) {
    Cell toAdd = maybeCloneWithAllocator(cell);
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
  @Override
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
  @Override
  public long timeOfOldestEdit() {
    return timeOfOldestEdit;
  }


  /**
   * Write a delete
   * @param deleteCell
   * @return approximate size of the passed key and value.
   */
  @Override
  public long delete(Cell deleteCell) {
    Cell toAdd = maybeCloneWithAllocator(deleteCell);
    long s = internalAdd(toAdd);
    return s;
  }

  /**
   * The passed snapshot was successfully persisted; it can be let go.
   * @param id Id of the snapshot to clean out.
   * @throws UnexpectedStateException
   * @see MemStore#snapshot(long)
   */
  @Override
  public void clearSnapshot(long id) throws UnexpectedStateException {
    if (this.snapshotId != id) {
      throw new UnexpectedStateException("Current snapshot id is " + this.snapshotId + ",passed "
          + id);
    }
    // OK. Passed in snapshot is same as current snapshot. If not-empty,
    // create a new snapshot and let the old one go.
    StoreSegment oldSnapshot = this.snapshot;
    if (!this.snapshot.isEmpty()) {
      this.snapshot = StoreSegmentFactory.instance().createImmutableSegment(
          getComparator(), 0);
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
    return getActive().getSize();
  }

  /**
   * On flush, how much memory we will clear from the active cell set.
   *
   * @return size of data that is going to be flushed from active set
   */
  @Override
  public long getFlushableSize() {
    long snapshotSize = getSnapshot().getSize();
    return snapshotSize > 0 ? snapshotSize : keySize();
  }


  /**
   * @return scanner on memstore and snapshot in this order.
   */
  @Override
  public List<KeyValueScanner> getScanners(long readPt) throws IOException {
    return Collections.<KeyValueScanner> singletonList(new MemStoreScanner(this, readPt));
  }

  @Override
  public long getSnapshotSize() {
    return getSnapshot().getSize();
  }

  @Override
  public String toString() {
    String res = "";
    int i = 1;
    try {
      for (StoreSegment segment : getListOfSegments()) {
        res += "Segment (" + i + ") " + segment.toString() + "; ";
        i++;
      }
    } catch (IOException e){
      return e.toString();
    }
    return res;
  }

  protected void rollbackSnapshot(Cell cell) {
    // If the key is in the snapshot, delete it. We should not update
    // this.size, because that tracks the size of only the memstore and
    // not the snapshot. The flush of this snapshot to disk has not
    // yet started because Store.flush() waits for all rwcc transactions to
    // commit before starting the flush to disk.
    snapshot.rollback(cell);
  }

  protected void rollbackActive(Cell cell) {
    // If the key is in the memstore, delete it. Update this.size.
    long sz = active.rollback(cell);
    if (sz != 0) {
      setOldestEditTimeToNow();
    }
  }

  protected Configuration getConfiguration() {
    return conf;
  }

  protected void dump(Log log) {
    active.dump(log);
    snapshot.dump(log);
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
   * @param readpoint
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
    SortedSet<Cell> ss = active.tailSet(firstCell);
    Iterator<Cell> it = ss.iterator();
    // versions visible to oldest scanner
    int versionsVisible = 0;
    while (it.hasNext()) {
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
            active.incSize(-delta);
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
  protected Cell getLowest(final Cell a, final Cell b) {
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
   * @param set Set to look in for a row beyond <code>row</code>.
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

  /**
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
  @VisibleForTesting
  @Override
  public long updateColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long newValue, long now) {
    Cell firstCell = KeyValueUtil.createFirstOnRow(row, family, qualifier);
    // Is there a Cell in 'snapshot' with the same TS? If so, upgrade the timestamp a bit.
    Cell snc = snapshot.getFirstAfter(firstCell);
    if(snc != null) {
      // is there a matching Cell in the snapshot?
      if (CellUtil.matchingRow(snc, firstCell) && CellUtil.matchingQualifier(snc, firstCell)) {
        if (snc.getTimestamp() == now) {
          now += 1;
        }
      }
    }
    // logic here: the new ts MUST be at least 'now'. But it could be larger if necessary.
    // But the timestamp should also be max(now, mostRecentTsInMemstore)

    // so we cant add the new Cell w/o knowing what's there already, but we also
    // want to take this chance to delete some cells. So two loops (sad)

    SortedSet<Cell> ss = getActive().tailSet(firstCell);
    for (Cell cell : ss) {
      // if this isnt the row we are interested in, then bail:
      if (!CellUtil.matchingColumn(cell, family, qualifier)
          || !CellUtil.matchingRow(cell, firstCell)) {
        break; // rows dont match, bail.
      }

      // if the qualifier matches and it's a put, just RM it out of the active.
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
    return active.maybeCloneWithAllocator(cell);
  }

  /**
   * Internal version of add() that doesn't clone Cells with the
   * allocator, and doesn't take the lock.
   *
   * Callers should ensure they already have the read lock taken
   */
  private long internalAdd(final Cell toAdd) {
    long s = active.add(toAdd);
    setOldestEditTimeToNow();
    return s;
  }

  private void setOldestEditTimeToNow() {
    if (timeOfOldestEdit == Long.MAX_VALUE) {
      timeOfOldestEdit = EnvironmentEdgeManager.currentTime();
    }
  }

  protected long keySize() {
    return heapSize() - deepOverhead();
  }

  protected CellComparator getComparator() {
    return comparator;
  }

  protected MutableSegment getActive() {
    return active;
  }

  protected ImmutableSegment getSnapshot() {
    return snapshot;
  }

  protected AbstractMemStore setSnapshot(ImmutableSegment snapshot) {
    this.snapshot = snapshot;
    return this;
  }

  protected void setSnapshotSize(long snapshotSize) {
    getSnapshot().setSize(snapshotSize);
  }

  /**
   * Returns a list of Store segment scanners, one per each store segment
   * @param readPt the version number required to initialize the scanners
   * @return a list of Store segment scanners, one per each store segment
   * @throws IOException
   */
  protected abstract List<StoreSegmentScanner> getListOfScanners(long readPt) throws IOException;

  /**
   * Returns an ordered list of segments from most recent to oldest in memstore
   * @return an ordered list of segments from most recent to oldest in memstore
   * @throws IOException
   */
  protected abstract List<StoreSegment> getListOfSegments() throws IOException;

}
