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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.cloudera.htrace.Trace;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

/**
 * The MemStore holds in-memory modifications to the Store.  Modifications
 * are {@link KeyValue}s.  When asked to flush, current memstore is moved
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
  }

  @Override protected long deepOverhead() {
    return DEEP_OVERHEAD;
  }

  void dump() {
    super.dump(LOG);
  }


  /**
   * Creates a snapshot of the current memstore.
   * Snapshot must be cleared by call to {@link #clearSnapshot}
   */
  @Override
  public void snapshot() {
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!getSnapshot().isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      this.snapshotId = EnvironmentEdgeManager.currentTimeMillis();
      if (!getCellSet().isEmpty()) {
        setSnapshot(getCellSet());
        setSnapshotSize(keySize());
        resetCellSet();
      }
    }
    //return new MemStoreSnapshot(this.snapshotId, getSnapshot(), getComparator());

  }

  @Override
  protected List<CellSetScanner> getListOfScanners(long readPt) throws IOException {
    List<CellSetScanner> list = new ArrayList<CellSetScanner>(2);
    list.add(0, getCellSet().getScanner(readPt));
    list.add(1, getSnapshot().getScanner(readPt));
    return list;
  }

  @Override public AbstractMemStore setForceFlush() {
    // do nothing
    return this;
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
  public void rollback(KeyValue cell) {
    rollbackSnapshot(cell);
    rollbackCellSet(cell);
  }

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately precedes it. The
   * target row key is set in state.
   *
   * @param state column/delete tracking state
   */
  @Override
  public void getRowKeyAtOrBefore(GetClosestRowBeforeTracker state) {
    getCellSet().getRowKeyAtOrBefore(state);
    getSnapshot().getRowKeyAtOrBefore(state);
  }

  /**
   * Check if this memstore may contain the required keys
   * @param scan
   * @return False if the key definitely does not exist in this memstore
   */
  @Override
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return
        (getCellSet().shouldSeek(scan, oldestUnexpiredTS) ||
            getSnapshot().shouldSeek(scan,oldestUnexpiredTS));
  }

  @Override
  public long getFlushableSize() {
    long snapshotSize = getSnapshot().getSize();
    return snapshotSize > 0 ? snapshotSize : keySize();
  }

  //methods for tests

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   * first.
   * @return Next row or null if none found.
   */
  KeyValue getNextRow(final KeyValue cell) {
    return getLowest(
        getNextRow(cell, getCellSet().getCellSet()),
        getNextRow(cell, getSnapshot().getCellSet()));
  }

  /**
   * @return Total memory occupied by this MemStore.
   */
  @Override public long size() {
    return heapSize();
  }

  /**
   * @return scanner on memstore and snapshot in this order.
   */
  public List<KeyValueScanner> getScanners(long readPt) throws IOException {
    return Collections.<KeyValueScanner> singletonList(new MemStoreScanner(this,
        readPt));
  }

  /*
   * MemStoreScanner implements the KeyValueScanner.
   * It lets the caller scan the contents of a memstore -- both current
   * map and snapshot.
   * This behaves as if it were a real scanner but does not maintain position.
   */
  protected class MemStoreScanner extends NonLazyKeyValueScanner {
    // Next row information for either kvset or snapshot
    private KeyValue kvsetNextRow = null;
    private KeyValue snapshotNextRow = null;

    // last iterated KVs for kvset and snapshot (to restore iterator state after reseek)
    private KeyValue kvsetItRow = null;
    private KeyValue snapshotItRow = null;

    // iterator based scanning.
    private Iterator<KeyValue> kvsetIt;
    private Iterator<KeyValue> snapshotIt;

    // The kvset and snapshot at the time of creating this scanner
    private CellSetMgr kvsetAtCreation;
    private CellSetMgr snapshotAtCreation;

    // the pre-calculated KeyValue to be returned by peek() or next()
    private KeyValue theNext;

    // The allocator and snapshot allocator at the time of creating this scanner
//    volatile MemStoreLAB allocatorAtCreation;
//    volatile MemStoreLAB snapshotAllocatorAtCreation;

    // A flag represents whether could stop skipping KeyValues for MVCC
    // if have encountered the next row. Only used for reversed scan
    private boolean stopSkippingKVsIfNextRow = false;

    private long readPoint;

    private final AbstractMemStore ms;
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

    MemStoreScanner(AbstractMemStore memStore, long readPoint) {
      super();
      ms = memStore;
      comparator = ms.getComparator();
      this.readPoint = readPoint;
      kvsetAtCreation = ms.getCellSet();
      kvsetAtCreation.incScannerCount();
      snapshotAtCreation = ms.getSnapshot();
      snapshotAtCreation.incScannerCount();
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

    private KeyValue getNext(Iterator<KeyValue> it) {
      KeyValue startKV = theNext;
      KeyValue v = null;
      try {
        while (it.hasNext()) {
          v = it.next();
          if (v.getMvccVersion() <= this.readPoint) {
            return v;
          }
          if (stopSkippingKVsIfNextRow && startKV != null
              && comparator.compareRows(v, startKV) > 0) {
            return null;
          }
        }

        return null;
      } finally {
        if (v != null) {
          // in all cases, remember the last KV iterated to
          if (it == snapshotIt) {
            snapshotItRow = v;
          } else {
            kvsetItRow = v;
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
    public synchronized boolean seek(KeyValue key) {
      if (key == null) {
        close();
        return false;
      }

      // kvset and snapshot will never be null.
      // if tailSet can't find anything, SortedSet is empty (not null).
      kvsetIt = kvsetAtCreation.tailSet(key).iterator();
      snapshotIt = snapshotAtCreation.tailSet(key).iterator();
      kvsetItRow = null;
      snapshotItRow = null;

      return seekInSubLists(key);
    }


    /**
     * (Re)initialize the iterators after a seek or a reseek.
     */
    private synchronized boolean seekInSubLists(KeyValue key){
      kvsetNextRow = getNext(kvsetIt);
      snapshotNextRow = getNext(snapshotIt);

      // Calculate the next value
      theNext = getLowest(kvsetNextRow, snapshotNextRow);

      // has data
      return (theNext != null);
    }


    /**
     * Move forward on the sub-lists set previously by seek.
     * @param key seek value (should be non-null)
     * @return true if there is at least one KV to read, false otherwise
     */
    @Override
    public synchronized boolean reseek(KeyValue key) {
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

      kvsetIt = kvsetAtCreation.tailSet(getHighest(key, kvsetItRow)).iterator();
      snapshotIt = snapshotAtCreation.tailSet(getHighest(key, snapshotItRow)).iterator();

      return seekInSubLists(key);
    }


    @Override
    public synchronized KeyValue peek() {
      //DebugPrint.println(" MS@" + hashCode() + " peek = " + getLowest());
      return theNext;
    }

    @Override
    public synchronized KeyValue next() {
      if (theNext == null) {
          return null;
      }

      final KeyValue ret = theNext;

      // Advance one of the iterators
      if (theNext == kvsetNextRow) {
        kvsetNextRow = getNext(kvsetIt);
      } else {
        snapshotNextRow = getNext(snapshotIt);
      }

      // Calculate the next value
      theNext = getLowest(kvsetNextRow, snapshotNextRow);

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
    private KeyValue getLowest(KeyValue first, KeyValue second) {
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
     * Returns the higher of the two key values, or null if they are both null.
     * This uses comparator.compare() to compare the KeyValue using the memstore
     * comparator.
     */
    private KeyValue getHighest(KeyValue first, KeyValue second) {
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
      this.kvsetNextRow = null;
      this.snapshotNextRow = null;

      this.kvsetIt = null;
      this.snapshotIt = null;

      kvsetAtCreation.decScannerCount();
      snapshotAtCreation.decScannerCount();

//      if (allocatorAtCreation != null) {
//        this.allocatorAtCreation.decScannerCount();
//        this.allocatorAtCreation = null;
//      }
//      if (snapshotAllocatorAtCreation != null) {
//        this.snapshotAllocatorAtCreation.decScannerCount();
//        this.snapshotAllocatorAtCreation = null;
//      }

      this.kvsetItRow = null;
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
    public synchronized boolean backwardSeek(KeyValue key) {
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
    public synchronized boolean seekToPreviousRow(KeyValue key) {
      KeyValue firstKeyOnRow = KeyValue.createFirstOnRow(key.getRow());
      SortedSet<KeyValue> kvHead = kvsetAtCreation.headSet(firstKeyOnRow);
      KeyValue kvsetBeforeRow = kvHead.isEmpty() ? null : kvHead.last();
      SortedSet<KeyValue> snapshotHead = snapshotAtCreation
          .headSet(firstKeyOnRow);
      KeyValue snapshotBeforeRow = snapshotHead.isEmpty() ? null : snapshotHead
          .last();
      KeyValue lastKVBeforeRow = getHighest(kvsetBeforeRow, snapshotBeforeRow);
      if (lastKVBeforeRow == null) {
        theNext = null;
        return false;
      }
      KeyValue firstKeyOnPreviousRow = KeyValue
          .createFirstOnRow(lastKVBeforeRow.getRow());
      this.stopSkippingKVsIfNextRow = true;
      seek(firstKeyOnPreviousRow);
      this.stopSkippingKVsIfNextRow = false;
      if (peek() == null
          || comparator.compareRows(peek(), firstKeyOnPreviousRow) > 0) {
        return seekToPreviousRow(lastKVBeforeRow);
      }
      return true;
    }

    @Override
    public synchronized boolean seekToLastRow() {
      KeyValue first = kvsetAtCreation.isEmpty() ? null : kvsetAtCreation
          .last();
      KeyValue second = snapshotAtCreation.isEmpty() ? null
          : snapshotAtCreation.last();
      KeyValue higherKv = getHighest(first, second);
      if (higherKv == null) {
        return false;
      }
      KeyValue firstKvOnLastRow = KeyValue.createFirstOnRow(higherKv.getRow());
      if (seek(firstKvOnLastRow)) {
        return true;
      } else {
        return seekToPreviousRow(higherKv);
      }

    }
  }


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
      size += memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
    }
    LOG.info("memstore1 estimated size=" + size);
    for (int i = 0; i < count; i++) {
      size += memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
    }
    LOG.info("memstore1 estimated size (2nd loading of same data)=" + size);
    // Make a variably sized memstore.
    DefaultMemStore memstore2 = new DefaultMemStore();
    for (int i = 0; i < count; i++) {
      size += memstore2.add(new KeyValue(Bytes.toBytes(i), fam, qf, i,
        new byte[i]));
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
