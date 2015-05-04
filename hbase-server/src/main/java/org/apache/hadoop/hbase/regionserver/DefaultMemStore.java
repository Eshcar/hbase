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
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.*;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import org.apache.htrace.Trace;


/**
 * The MemStore holds in-memory modifications to the Store.  Modifications
 * are {@link Cell}s.  When asked to flush, current memstore is moved
 * to snapshot and is cleared.  We continue to serve edits out of new memstore
 * and backing snapshot until flusher reports in that the flush succeeded. At
 * this point we let the snapshot go.
 *  <p>
 * The MemStore functions should not be called in parallel. Callers should hold
 *  write and read locks. This is done in {@link HStore}.
 *  </p>
 *
 * TODO: Adjust size of the memstore when we remove items because they have
 * been deleted.
 * TODO: With new KVSLS, need to make sure we update HeapSize with difference
 * in KV size.
 */
@InterfaceAudience.Private
public class DefaultMemStore extends AbstractMemStore {
  private static final Log LOG = LogFactory.getLog(DefaultMemStore.class);

  // Snapshot of memstore.  Made for flusher.
  volatile CellSetMgr snapshot;

  // Used to track own heapSize
  private volatile long snapshotSize;

  TimeRangeTracker snapshotTimeRangeTracker;

  volatile long snapshotId;

  public final static long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT +
      (7 * ClassSize.REFERENCE) + // 2 in DefaultMemStore, 5 in AbstractMemStore
      (3 * Bytes.SIZEOF_LONG));   // 2 in DefaultMemStore, 1 in AbstractMemStore

  public final static long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
      ClassSize.ATOMIC_LONG + (2 * ClassSize.TIMERANGE_TRACKER) +
      (2 * ClassSize.CELL_SKIPLIST_SET) + (2 * ClassSize.CONCURRENT_SKIPLISTMAP));


  /**
   * Default constructor. Used for tests.
   */
  public DefaultMemStore() {
    this(HBaseConfiguration.create(), KeyValue.COMPARATOR);
  }

  /**
   * Constructor.
   * @param c Comparator
   */
  public DefaultMemStore(final Configuration conf, final KeyValue.KVComparator c) {
    super(conf, c);
    this.snapshot = CellSetMgr.Factory.instance().createCellSetMgr(
        CellSetMgr.Type.EMPTY_SNAPSHOT, conf,c);
    snapshotTimeRangeTracker = new TimeRangeTracker();
    this.snapshotSize = 0;
  }

  @Override protected long deepOverhead() {
    return DEEP_OVERHEAD;
  }

  void dump() {
    super.dump(LOG);
    for (Cell cell: this.snapshot.getCellSet()) {
      LOG.info(cell);
    }
  }

  /**
   * Creates a snapshot of the current memstore.
   * Snapshot must be cleared by call to {@link #clearSnapshot(long)}
   */
  @Override
  public MemStoreSnapshot snapshot() {
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!this.snapshot.isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      this.snapshotId = EnvironmentEdgeManager.currentTime();
      this.snapshotSize = keySize();
      if (!getCellSet().isEmpty()) {
        this.snapshot = getCellSet();
        this.snapshotTimeRangeTracker = this.getTimeRangeTracker();
        resetCellSet();
      }
    }
    return new MemStoreSnapshot(this.snapshotId, snapshot.size(), this.snapshotSize,
        this.snapshotTimeRangeTracker, new CollectionBackedScanner(snapshot.getCellSet(), getComparator()));
  }

  /**
   * The passed snapshot was successfully persisted; it can be let go.
   * @param id Id of the snapshot to clean out.
   * @throws UnexpectedStateException
   * @see #snapshot()
   */
  @Override
  public void clearSnapshot(long id) throws UnexpectedStateException {
    if (this.snapshotId != id) {
      throw new UnexpectedStateException("Current snapshot id is " + this.snapshotId + ",passed "
          + id);
    }
    // OK. Passed in snapshot is same as current snapshot. If not-empty,
    // create a new snapshot and let the old one go.
    if (!this.snapshot.isEmpty()) {
      this.snapshot.close();
      this.snapshot = CellSetMgr.Factory.instance().createCellSetMgr(
          CellSetMgr.Type.EMPTY_SNAPSHOT, getComparator());
      this.snapshotTimeRangeTracker = new TimeRangeTracker();
    }
    this.snapshotSize = 0;
    this.snapshotId = -1;
  }

  @Override
  public long getFlushableSize() {
    return this.snapshotSize > 0 ? this.snapshotSize : keySize();
  }

  @Override
  public long getSnapshotSize() {
    return this.snapshotSize;
  }

  /**
   * Remove n key from the memstore. Only cells that have the same key and the
   * same memstoreTS are removed.  It is ok to not update timeRangeTracker
   * in this call. It is possible that we can optimize this method by using
   * tailMap/iterator, but since this method is called rarely (only for
   * error recovery), we can leave those optimization for the future.
   * @param cell
   */
  @Override
  public void rollback(Cell cell) {
    // If the key is in the snapshot, delete it. We should not update
    // this.size, because that tracks the size of only the memstore and
    // not the snapshot. The flush of this snapshot to disk has not
    // yet started because Store.flush() waits for all rwcc transactions to
    // commit before starting the flush to disk.
    Cell found = this.snapshot.get(cell);
    if (found != null && found.getSequenceId() == cell.getSequenceId()) {
      this.snapshot.remove(cell);
      long sz = heapSizeChange(cell, true);
      this.snapshotSize -= sz;
    }
    super.rollback(cell);
  }

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   * first.
   * @return Next row or null if none found.
   */
  @Override
  Cell getNextRow(final Cell cell) {
    return getLowest(super.getNextRow(cell), getNextRow(cell, this.snapshot.getCellSet()));
  }


  /**
   * @param state column/delete tracking state
   */
  @Override
  public void getRowKeyAtOrBefore(final GetClosestRowBeforeTracker state) {
    super.getRowKeyAtOrBefore(state);
    getRowKeyAtOrBefore(snapshot.getCellSet(), state);
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
  @Override
  public long updateColumnValue(byte[] row,
                                byte[] family,
                                byte[] qualifier,
                                long newValue,
                                long now) {
    Cell firstCell = KeyValueUtil.createFirstOnRow(row, family, qualifier);
    // Is there a Cell in 'snapshot' with the same TS? If so, upgrade the timestamp a bit.
    SortedSet<Cell> snSs = snapshot.tailSet(firstCell);
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
    return super.updateColumnValue(row, family, qualifier, newValue, now, firstCell);
  }

  /**
   * @return scanner on memstore and snapshot in this order.
   */
  @Override
  public List<KeyValueScanner> getScanners(long readPt) {
    return Collections.<KeyValueScanner> singletonList(new MemStoreScanner(this, readPt));
  }

  /**
   * Check if this memstore may contain the required keys
   * @param scan
   * @return False if the key definitely does not exist in this Memstore
   */
  @Override
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return (super.shouldSeek(scan, oldestUnexpiredTS) ||
        ((snapshotTimeRangeTracker.includesTimeRange(scan.getTimeRange()))
        && (snapshotTimeRangeTracker.getMaximumTimestamp() >=
            oldestUnexpiredTS)));
  }

  /*
   * MemStoreScanner implements the KeyValueScanner.
   * It lets the caller scan the contents of a memstore -- both current
   * map and snapshot.
   * This behaves as if it were a real scanner but does not maintain position.
   */
  protected class MemStoreScanner extends NonLazyKeyValueScanner {
    // Next row information for either cellSet or snapshot
    private Cell cellSetNextRow = null;
    private Cell snapshotNextRow = null;

    // last iterated Cells for cellSet and snapshot (to restore iterator state after reseek)
    private Cell cellSetItRow = null;
    private Cell snapshotItRow = null;

    // iterator based scanning.
    private Iterator<Cell> cellSetIt;
    private Iterator<Cell> snapshotIt;

    // The cellSet and snapshot at the time of creating this scanner
    private CellSet cellSetAtCreation;
    private CellSet snapshotAtCreation;

    // the pre-calculated Cell to be returned by peek() or next()
    private Cell theNext;

    // The allocator and snapshot allocator at the time of creating this scanner
//    volatile MemStoreLAB allocatorAtCreation;
//    volatile MemStoreLAB snapshotAllocatorAtCreation;

    // A flag represents whether could stop skipping Cells for MVCC
    // if have encountered the next row. Only used for reversed scan
    private boolean stopSkippingCellsIfNextRow = false;

    private long readPoint;

    private final KeyValue.KVComparator comparator;

    /*
    Some notes...

     So memstorescanner is fixed at creation time. this includes pointers/iterators into
    existing kvset/snapshot.  during a snapshot creation, the kvset is null, and the
    snapshot is moved.  since kvset is null there is no point on reseeking on both,
      we can save us the trouble. During the snapshot->hfile transition, the memstore
      scanner is re-created by StoreScanner#updateReaders().  StoreScanner should
      potentially do something smarter by adjusting the existing memstore scanner.

      But there is a greater problem here, that being once a scanner has progressed
      during a snapshot scenario, we currently iterate past the kvset then 'finish' up.
      if a scan lasts a little while, there is a chance for new entries in kvset to
      become available but we will never see them.  This needs to be handled at the
      StoreScanner level with coordination with MemStoreScanner.

      Currently, this problem is only partly managed: during the small amount of time
      when the StoreScanner has not yet created a new MemStoreScanner, we will miss
      the adds to kvset in the MemStoreScanner.
    */

    MemStoreScanner(DefaultMemStore defaultMemStore, long readPoint) {
      super();
      comparator = defaultMemStore.getComparator();
      this.readPoint = readPoint;
      cellSetAtCreation = getCellSet().getCellSet();
      getCellSet().incScannerCount();
      snapshotAtCreation = snapshot.getCellSet();
      snapshot.incScannerCount();
//      if (allocator != null) {
//        this.allocatorAtCreation = allocator;
//        this.allocatorAtCreation.incScannerCount();
//      }
//      if (snapshotAllocator != null) {
//        this.snapshotAllocatorAtCreation = snapshotAllocator;
//        this.snapshotAllocatorAtCreation.incScannerCount();
//      }
      if (Trace.isTracing() && Trace.currentSpan() != null) {
        Trace.currentSpan().addTimelineAnnotation("Creating MemStoreScanner");
      }
    }

    /**
     * Lock on 'this' must be held by caller.
     * @param it
     * @return Next Cell
     */
    private Cell getNext(Iterator<Cell> it) {
      Cell startCell = theNext;
      Cell v = null;
      try {
        while (it.hasNext()) {
          v = it.next();
          if (v.getSequenceId() <= this.readPoint) {
            return v;
          }
          if (stopSkippingCellsIfNextRow && startCell != null
              && getComparator().compareRows(v, startCell) > 0) {
            return null;
          }
        }

        return null;
      } finally {
        if (v != null) {
          // in all cases, remember the last Cell iterated to
          if (it == snapshotIt) {
            snapshotItRow = v;
          } else {
            cellSetItRow = v;
          }
        }
      }
    }

    /**
     *  Set the scanner at the seek key.
     *  Must be called only once: there is no thread safety between the scanner
     *   and the memStore.
     * @param key seek value
     * @return false if the key is null or if there is no data
     */
    @Override
    public synchronized boolean seek(Cell key) {
      if (key == null) {
        close();
        return false;
      }
      // kvset and snapshot will never be null.
      // if tailSet can't find anything, SortedSet is empty (not null).
      cellSetIt = cellSetAtCreation.tailSet(key).iterator();
      snapshotIt = snapshotAtCreation.tailSet(key).iterator();
      cellSetItRow = null;
      snapshotItRow = null;

      return seekInSubLists(key);
    }


    /**
     * (Re)initialize the iterators after a seek or a reseek.
     */
    private synchronized boolean seekInSubLists(Cell key){
      cellSetNextRow = getNext(cellSetIt);
      snapshotNextRow = getNext(snapshotIt);

      // Calculate the next value
      theNext = getLowest(cellSetNextRow, snapshotNextRow);

      // has data
      return (theNext != null);
    }


    /**
     * Move forward on the sub-lists set previously by seek.
     * @param key seek value (should be non-null)
     * @return true if there is at least one KV to read, false otherwise
     */
    @Override
    public synchronized boolean reseek(Cell key) {
      /*
      See HBASE-4195 & HBASE-3855 & HBASE-6591 for the background on this implementation.
      This code is executed concurrently with flush and puts, without locks.
      Two points must be known when working on this code:
      1) It's not possible to use the 'kvTail' and 'snapshot'
       variables, as they are modified during a flush.
      2) The ideal implementation for performance would use the sub skip list
       implicitly pointed by the iterators 'kvsetIt' and
       'snapshotIt'. Unfortunately the Java API does not offer a method to
       get it. So we remember the last keys we iterated to and restore
       the reseeked set to at least that point.
       */
      cellSetIt = cellSetAtCreation.tailSet(getHighest(key, cellSetItRow)).iterator();
      snapshotIt = snapshotAtCreation.tailSet(getHighest(key, snapshotItRow)).iterator();

      return seekInSubLists(key);
    }


    @Override
    public synchronized Cell peek() {
      //DebugPrint.println(" MS@" + hashCode() + " peek = " + getLowest());
      return theNext;
    }

    @Override
    public synchronized Cell next() {
      if (theNext == null) {
          return null;
      }

      final Cell ret = theNext;

      // Advance one of the iterators
      if (theNext == cellSetNextRow) {
        cellSetNextRow = getNext(cellSetIt);
      } else {
        snapshotNextRow = getNext(snapshotIt);
      }

      // Calculate the next value
      theNext = getLowest(cellSetNextRow, snapshotNextRow);

      //long readpoint = ReadWriteConsistencyControl.getThreadReadPoint();
      //DebugPrint.println(" MS@" + hashCode() + " next: " + theNext + " next_next: " +
      //    getLowest() + " threadpoint=" + readpoint);
      return ret;
    }

    /*
     * Returns the lower of the two key values, or null if they are both null.
     * This uses comparator.compare() to compare the KeyValue using the memstore
     * comparator.
     */
    private Cell getLowest(Cell first, Cell second) {
      if (first == null && second == null) {
        return null;
      }
      if (first != null && second != null) {
        int compare = comparator.compare(first, second);
        return (compare <= 0 ? first : second);
      }
      return (first != null ? first : second);
    }

    /*
     * Returns the higher of the two cells, or null if they are both null.
     * This uses comparator.compare() to compare the Cell using the memstore
     * comparator.
     */
    private Cell getHighest(Cell first, Cell second) {
      if (first == null && second == null) {
        return null;
      }
      if (first != null && second != null) {
        int compare = comparator.compare(first, second);
        return (compare > 0 ? first : second);
      }
      return (first != null ? first : second);
    }

    public synchronized void close() {
      this.cellSetNextRow = null;
      this.snapshotNextRow = null;

      this.cellSetIt = null;
      this.snapshotIt = null;

      getCellSet().decScannerCount();
      snapshot.decScannerCount();

//      if (allocatorAtCreation != null) {
//        this.allocatorAtCreation.decScannerCount();
//        this.allocatorAtCreation = null;
//      }
//      if (snapshotAllocatorAtCreation != null) {
//        this.snapshotAllocatorAtCreation.decScannerCount();
//        this.snapshotAllocatorAtCreation = null;
//      }

      this.cellSetItRow = null;
      this.snapshotItRow = null;
    }

    /**
     * MemStoreScanner returns max value as sequence id because it will
     * always have the latest data among all files.
     */
    @Override
    public long getSequenceID() {
      return Long.MAX_VALUE;
    }

    @Override
    public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns,
        long oldestUnexpiredTS) {
      return shouldSeek(scan, oldestUnexpiredTS);
    }

    /**
     * Seek scanner to the given key first. If it returns false(means
     * peek()==null) or scanner's peek row is bigger than row of given key, seek
     * the scanner to the previous row of given key
     */
    @Override
    public synchronized boolean backwardSeek(Cell key) {
      seek(key);
      if (peek() == null || comparator.compareRows(peek(), key) > 0) {
        return seekToPreviousRow(key);
      }
      return true;
    }

    /**
     * Separately get the KeyValue before the specified key from kvset and
     * snapshotset, and use the row of higher one as the previous row of
     * specified key, then seek to the first KeyValue of previous row
     */
    @Override
    public synchronized boolean seekToPreviousRow(Cell key) {
      Cell firstKeyOnRow = KeyValueUtil.createFirstOnRow(key.getRowArray(), key.getRowOffset(),
          key.getRowLength());
      SortedSet<Cell> cellHead = cellSetAtCreation.headSet(firstKeyOnRow);
      Cell cellSetBeforeRow = cellHead.isEmpty() ? null : cellHead.last();
      SortedSet<Cell> snapshotHead = snapshotAtCreation
          .headSet(firstKeyOnRow);
      Cell snapshotBeforeRow = snapshotHead.isEmpty() ? null : snapshotHead
          .last();
      Cell lastCellBeforeRow = getHighest(cellSetBeforeRow, snapshotBeforeRow);
      if (lastCellBeforeRow == null) {
        theNext = null;
        return false;
      }
      Cell firstKeyOnPreviousRow = KeyValueUtil.createFirstOnRow(lastCellBeforeRow.getRowArray(),
          lastCellBeforeRow.getRowOffset(), lastCellBeforeRow.getRowLength());
      this.stopSkippingCellsIfNextRow = true;
      seek(firstKeyOnPreviousRow);
      this.stopSkippingCellsIfNextRow = false;
      if (peek() == null
          || comparator.compareRows(peek(), firstKeyOnPreviousRow) > 0) {
        return seekToPreviousRow(lastCellBeforeRow);
      }
      return true;
    }

    @Override
    public synchronized boolean seekToLastRow() {
      Cell first = cellSetAtCreation.isEmpty() ? null : cellSetAtCreation
          .last();
      Cell second = snapshotAtCreation.isEmpty() ? null
          : snapshotAtCreation.last();
      Cell higherCell = getHighest(first, second);
      if (higherCell == null) {
        return false;
      }
      Cell firstCellOnLastRow = KeyValueUtil.createFirstOnRow(higherCell.getRowArray(),
          higherCell.getRowOffset(), higherCell.getRowLength());
      if (seek(firstCellOnLastRow)) {
        return true;
      } else {
        return seekToPreviousRow(higherCell);
      }

    }
  }

//  public final static long FIXED_OVERHEAD = ClassSize.align(
//      ClassSize.OBJECT + (9 * ClassSize.REFERENCE) + (3 * Bytes.SIZEOF_LONG));
//
//  public final static long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
//      ClassSize.ATOMIC_LONG + (2 * ClassSize.TIMERANGE_TRACKER) +
//      (2 * ClassSize.CELL_SKIPLIST_SET) + (2 * ClassSize.CONCURRENT_SKIPLISTMAP));
//
//  /*
//   * Calculate how the MemStore size has changed.  Includes overhead of the
//   * backing Map.
//   * @param cell
//   * @param notpresent True if the cell was NOT present in the set.
//   * @return Size
//   */
//  static long heapSizeChange(final Cell cell, final boolean notpresent) {
//    return notpresent ? ClassSize.align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY
//        + CellUtil.estimatedHeapSizeOf(cell)) : 0;
//  }

//  private long keySize() {
//    return heapSize() - DEEP_OVERHEAD;
//  }
//
//  /**
//   * Get the entire heap usage for this MemStore not including keys in the
//   * snapshot.
//   */
//  @Override
//  public long heapSize() {
//    return size.get();
//  }
//
//  @Override
//  public long size() {
//    return heapSize();
//  }

  /**
   * Code to help figure if our approximation of object heap sizes is close
   * enough.  See hbase-900.  Fills memstores then waits so user can heap
   * dump and bring up resultant hprof in something like jprofiler which
   * allows you get 'deep size' on objects.
   * @param args main args
   */
  public static void main(String [] args) {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
      runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
    LOG.info("vmInputArguments=" + runtime.getInputArguments());
    DefaultMemStore memstore1 = new DefaultMemStore();
    // TODO: x32 vs x64
    long size = 0;
    final int count = 10000;
    byte [] fam = Bytes.toBytes("col");
    byte [] qf = Bytes.toBytes("umn");
    byte [] empty = new byte[0];
    for (int i = 0; i < count; i++) {
      // Give each its own ts
      Pair<Long, Cell> ret = memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
      size += ret.getFirst();
    }
    LOG.info("memstore1 estimated size=" + size);
    for (int i = 0; i < count; i++) {
      Pair<Long, Cell> ret = memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
      size += ret.getFirst();
    }
    LOG.info("memstore1 estimated size (2nd loading of same data)=" + size);
    // Make a variably sized memstore.
    DefaultMemStore memstore2 = new DefaultMemStore();
    for (int i = 0; i < count; i++) {
      Pair<Long, Cell> ret = memstore2.add(new KeyValue(Bytes.toBytes(i), fam, qf, i,
        new byte[i]));
      size += ret.getFirst();
    }
    LOG.info("memstore2 estimated size=" + size);
    final int seconds = 30;
    LOG.info("Waiting " + seconds + " seconds while heap dump is taken");
    for (int i = 0; i < seconds; i++) {
      // Thread.sleep(1000);
    }
    LOG.info("Exiting.");
  }

}
