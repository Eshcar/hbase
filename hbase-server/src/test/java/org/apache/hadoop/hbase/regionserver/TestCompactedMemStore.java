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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * compacted memstore test case
 */
@Category(MediumTests.class)
public class TestCompactedMemStore extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestCompactedMemStore.class);
  private static final int ROW_COUNT = 10;
  private static final int QUALIFIER_COUNT = ROW_COUNT;
  private static final byte[] FAMILY = Bytes.toBytes("column");
  private static MemStoreChunkPool chunkPool;
  private CompactedMemStore cms;
  private HRegion region;
  private HStore store;
  private MultiVersionConsistencyControl mvcc;
  private AtomicLong startSeqNum = new AtomicLong(0);

  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////
  private static byte[] makeQualifier(final int i1, final int i2) {
    return Bytes.toBytes(Integer.toString(i1) + ";" +
        Integer.toString(i2));
  }

  //  private KeyValue getDeleteKV(byte [] row) {
  //    return new KeyValue(row, Bytes.toBytes("test_col"), null,
  //      HConstants.LATEST_TIMESTAMP, KeyValue.Type.Delete, null);
  //  }
  //
  //  private KeyValue getKV(byte [] row, byte [] value) {
  //    return new KeyValue(row, Bytes.toBytes("test_col"), null,
  //      HConstants.LATEST_TIMESTAMP, value);
  //  }
  private static void addRows(int count, final CompactedMemStore mem) {
    long nanos = System.nanoTime();

    for (int i = 0; i < count; i++) {
      if (i % 1000 == 0) {

        System.out.println(i + " Took for 1k usec: " + (System.nanoTime() - nanos) / 1000);
        nanos = System.nanoTime();
      }
      long timestamp = System.currentTimeMillis();

      for (int ii = 0; ii < QUALIFIER_COUNT; ii++) {
        byte[] row = Bytes.toBytes(i);
        byte[] qf = makeQualifier(i, ii);
        mem.add(new KeyValue(row, FAMILY, qf, timestamp, qf));
      }
    }
  }

  static void doScan(AbstractMemStore ms, int iteration) throws IOException {
    long nanos = System.nanoTime();
    KeyValueScanner s = ms.getScanners(0).get(0);
    s.seek(KeyValueUtil.createFirstOnRow(new byte[] { }));

    System.out.println(iteration + " create/seek took: " + (System.nanoTime() - nanos) / 1000);
    int cnt = 0;
    while (s.next() != null) ++cnt;

    System.out.println(iteration + " took usec: " + (System.nanoTime() - nanos) / 1000 + " for: "
        + cnt);

  }

  @Override
  public void tearDown() throws Exception {
    chunkPool.clearChunks();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.mvcc = new MultiVersionConsistencyControl();
    Configuration conf = new Configuration();
    conf.setBoolean(MemStoreSegment.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreChunkPool.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 1000);
    HBaseTestingUtility hbaseUtility = HBaseTestingUtility.createLocalHTU(conf);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    this.region = hbaseUtility.createTestRegion("foobar", hcd);
    this.store = new HStore(region, hcd, conf);
    this.cms = new CompactedMemStore(HBaseConfiguration.create(), CellComparator.COMPARATOR, store);
    chunkPool = MemStoreChunkPool.getPool(conf);
    assertTrue(chunkPool != null);
  }

  public void testPutSameKey() {
    byte[] bytes = Bytes.toBytes(getName());
    KeyValue kv = new KeyValue(bytes, bytes, bytes, bytes);
    this.cms.add(kv);
    byte[] other = Bytes.toBytes("somethingelse");
    KeyValue samekey = new KeyValue(bytes, bytes, bytes, other);
    this.cms.add(samekey);
    Cell found = this.cms.getActive().first();
    assertEquals(1, this.cms.getActive().getCellsCount());
    assertTrue(Bytes.toString(found.getValue()), CellUtil.matchingValue(samekey, found));
  }

  /**
   * Test memstore snapshot happening while scanning.
   *
   * @throws IOException
   */
  public void testScanAcrossSnapshot() throws IOException {
    int rowCount = addRows(this.cms);
    List<KeyValueScanner> memstorescanners = this.cms.getScanners(0);
    Scan scan = new Scan();
    List<Cell> result = new ArrayList<Cell>();
    ScanInfo scanInfo =
        new ScanInfo(null, 0, 1, HConstants.LATEST_TIMESTAMP, KeepDeletedCells.FALSE, 0,
            this.cms.getComparator());
    ScanType scanType = ScanType.USER_SCAN;
    StoreScanner s = new StoreScanner(scan, scanInfo, scanType, null, memstorescanners);
    int count = 0;
    try {
      while (s.next(result)) {
        LOG.info(result);
        count++;
        // Row count is same as column count.
        assertEquals(rowCount, result.size());
        result.clear();
      }
    } finally {
      s.close();
    }
    assertEquals(rowCount, count);
    for (KeyValueScanner scanner : memstorescanners) {
      scanner.close();
    }

    memstorescanners = this.cms.getScanners(mvcc.memstoreReadPoint());
    // Now assert can count same number even if a snapshot mid-scan.
    s = new StoreScanner(scan, scanInfo, scanType, null, memstorescanners);
    count = 0;
    try {
      while (s.next(result)) {
        LOG.info(result);
        // Assert the stuff is coming out in right order.
        assertTrue(CellUtil.matchingRow(result.get(0), Bytes.toBytes(count)));
        count++;
        // Row count is same as column count.
        assertEquals(rowCount, result.size());
        if (count == 2) {
          // the test should be still correct although the compaction is starting in the background
          // there should be nothing to compact
          this.cms.snapshot();
          LOG.info("Snapshotted");
        }
        result.clear();
      }
    } finally {
      s.close();
    }

    // snapshot immediately starts compaction, but even with the compaction nothing
    // should be compacted (unique keys) and the test should still be correct...
    assertEquals(rowCount, count);
    for (KeyValueScanner scanner : memstorescanners) {
      scanner.close();
    }
    memstorescanners = this.cms.getScanners(mvcc.memstoreReadPoint());
    // Assert that new values are seen in kvset as we scan.
    long ts = System.currentTimeMillis();
    s = new StoreScanner(scan, scanInfo, scanType, null, memstorescanners);
    count = 0;
    int snapshotIndex = 5;
    try {
      while (s.next(result)) {
        LOG.info(result);
        // Assert the stuff is coming out in right order.
        assertTrue(CellUtil.matchingRow(result.get(0), Bytes.toBytes(count)));
        // Row count is same as column count.
        assertEquals("count=" + count + ", result=" + result, rowCount, result.size());
        count++;
        if (count == snapshotIndex) {
          MemStoreSnapshot snapshot = this.cms.snapshot();
          this.cms.clearSnapshot(snapshot.getId());
          // Added more rows into kvset.  But the scanner wont see these rows.
          addRows(this.cms, ts);
          LOG.info("Snapshotted, cleared it and then added values (which wont be seen)");
        }
        result.clear();
      }
    } finally {
      s.close();
    }
    assertEquals(rowCount, count);
  }

  /**
   * A simple test which verifies the 3 possible states when scanning across snapshot.
   *
   * @throws IOException
   * @throws CloneNotSupportedException
   */
  public void testScanAcrossSnapshot2() throws IOException, CloneNotSupportedException {
    // we are going to the scanning across snapshot with two kvs
    // kv1 should always be returned before kv2
    final byte[] one = Bytes.toBytes(1);
    final byte[] two = Bytes.toBytes(2);
    final byte[] f = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    final byte[] v = Bytes.toBytes(3);

    final KeyValue kv1 = new KeyValue(one, f, q, 10, v);
    final KeyValue kv2 = new KeyValue(two, f, q, 10, v);

    // use case 1: both kvs in kvset
    this.cms.add(kv1.clone());
    this.cms.add(kv2.clone());
    verifyScanAcrossSnapshot2(kv1, kv2);

    // use case 2: both kvs in snapshot
    this.cms.snapshot();
    verifyScanAcrossSnapshot2(kv1, kv2);

    // use case 3: first in snapshot second in kvset
    this.cms = new CompactedMemStore(HBaseConfiguration.create(),
        CellComparator.COMPARATOR, store);
    this.cms.add(kv1.clone());
    this.cms
        .snapshot();                    // As compaction is starting in the background the repetition
    this.cms.add(
        kv2.clone());              // of the k1 might be removed BUT the scanners created earlier
    verifyScanAcrossSnapshot2(kv1,
        kv2);    // should look on the OLD MemStoreSegment, so this should be OK...
  }

  private void verifyScanAcrossSnapshot2(KeyValue kv1, KeyValue kv2)
      throws IOException {
    List<KeyValueScanner> memstorescanners = this.cms.getScanners(mvcc.memstoreReadPoint());
    assertEquals(1, memstorescanners.size());
    final KeyValueScanner scanner = memstorescanners.get(0);
    scanner.seek(KeyValueUtil.createFirstOnRow(HConstants.EMPTY_START_ROW));
    assertEquals(kv1, scanner.next());
    assertEquals(kv2, scanner.next());
    assertNull(scanner.next());
  }

  private void assertScannerResults(KeyValueScanner scanner, KeyValue[] expected)
      throws IOException {
    scanner.seek(KeyValueUtil.createFirstOnRow(new byte[] { }));
    List<Cell> returned = Lists.newArrayList();

    while (true) {
      Cell next = scanner.next();
      if (next == null) break;
      returned.add(next);
    }

    assertTrue(
        "Got:\n" + Joiner.on("\n").join(returned) +
            "\nExpected:\n" + Joiner.on("\n").join(expected),
        Iterables.elementsEqual(Arrays.asList(expected), returned));
    assertNull(scanner.peek());
  }

  public void testMemstoreConcurrentControl() throws IOException {
    final byte[] row = Bytes.toBytes(1);
    final byte[] f = Bytes.toBytes("family");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] v = Bytes.toBytes("value");

    MultiVersionConsistencyControl.WriteEntry w =
        mvcc.beginMemstoreInsert();

    KeyValue kv1 = new KeyValue(row, f, q1, v);
    kv1.setSequenceId(w.getWriteNumber());
    cms.add(kv1);

    KeyValueScanner s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { });

    mvcc.completeMemstoreInsert(w);

    s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { kv1 });

    w = mvcc.beginMemstoreInsert();
    KeyValue kv2 = new KeyValue(row, f, q2, v);
    kv2.setSequenceId(w.getWriteNumber());
    cms.add(kv2);

    s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { kv1 });

    mvcc.completeMemstoreInsert(w);

    s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { kv1, kv2 });
  }

  /**
   * Regression test for HBASE-2616, HBASE-2670.
   * When we insert a higher-memstoreTS version of a cell but with
   * the same timestamp, we still need to provide consistent reads
   * for the same scanner.
   */
  public void testMemstoreEditsVisibilityWithSameKey() throws IOException {
    final byte[] row = Bytes.toBytes(1);
    final byte[] f = Bytes.toBytes("family");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] v1 = Bytes.toBytes("value1");
    final byte[] v2 = Bytes.toBytes("value2");

    // INSERT 1: Write both columns val1
    MultiVersionConsistencyControl.WriteEntry w =
        mvcc.beginMemstoreInsert();

    KeyValue kv11 = new KeyValue(row, f, q1, v1);
    kv11.setSequenceId(w.getWriteNumber());
    cms.add(kv11);

    KeyValue kv12 = new KeyValue(row, f, q2, v1);
    kv12.setSequenceId(w.getWriteNumber());
    cms.add(kv12);
    mvcc.completeMemstoreInsert(w);

    // BEFORE STARTING INSERT 2, SEE FIRST KVS
    KeyValueScanner s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { kv11, kv12 });

    // START INSERT 2: Write both columns val2
    w = mvcc.beginMemstoreInsert();
    KeyValue kv21 = new KeyValue(row, f, q1, v2);
    kv21.setSequenceId(w.getWriteNumber());
    cms.add(kv21);

    KeyValue kv22 = new KeyValue(row, f, q2, v2);
    kv22.setSequenceId(w.getWriteNumber());
    cms.add(kv22);

    // BEFORE COMPLETING INSERT 2, SEE FIRST KVS
    s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { kv11, kv12 });

    // COMPLETE INSERT 2
    mvcc.completeMemstoreInsert(w);

    // NOW SHOULD SEE NEW KVS IN ADDITION TO OLD KVS.
    // See HBASE-1485 for discussion about what we should do with
    // the duplicate-TS inserts
    s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { kv21, kv11, kv22, kv12 });
  }

  /**
   * When we insert a higher-memstoreTS deletion of a cell but with
   * the same timestamp, we still need to provide consistent reads
   * for the same scanner.
   */
  public void testMemstoreDeletesVisibilityWithSameKey() throws IOException {
    final byte[] row = Bytes.toBytes(1);
    final byte[] f = Bytes.toBytes("family");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] v1 = Bytes.toBytes("value1");
    // INSERT 1: Write both columns val1
    MultiVersionConsistencyControl.WriteEntry w =
        mvcc.beginMemstoreInsert();

    KeyValue kv11 = new KeyValue(row, f, q1, v1);
    kv11.setSequenceId(w.getWriteNumber());
    cms.add(kv11);

    KeyValue kv12 = new KeyValue(row, f, q2, v1);
    kv12.setSequenceId(w.getWriteNumber());
    cms.add(kv12);
    mvcc.completeMemstoreInsert(w);

    // BEFORE STARTING INSERT 2, SEE FIRST KVS
    KeyValueScanner s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { kv11, kv12 });

    // START DELETE: Insert delete for one of the columns
    w = mvcc.beginMemstoreInsert();
    KeyValue kvDel = new KeyValue(row, f, q2, kv11.getTimestamp(),
        KeyValue.Type.DeleteColumn);
    kvDel.setSequenceId(w.getWriteNumber());
    cms.add(kvDel);

    // BEFORE COMPLETING DELETE, SEE FIRST KVS
    s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { kv11, kv12 });

    // COMPLETE DELETE
    mvcc.completeMemstoreInsert(w);

    // NOW WE SHOULD SEE DELETE
    s = this.cms.getScanners(mvcc.memstoreReadPoint()).get(0);
    assertScannerResults(s, new KeyValue[] { kv11, kvDel, kv12 });
  }

  public void testReadOwnWritesUnderConcurrency() throws Throwable {

    int NUM_THREADS = 8;

    ReadOwnWritesTester threads[] = new ReadOwnWritesTester[NUM_THREADS];
    AtomicReference<Throwable> caught = new AtomicReference<Throwable>();

    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i] = new ReadOwnWritesTester(i, cms, mvcc, caught, this.startSeqNum);
      threads[i].start();
    }

    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i].join();
    }

    if (caught.get() != null) {
      throw caught.get();
    }
  }

  ///////////////////////////////-/-/-/-////////////////////////////////////////////
  // Get tests
  //////////////////////////////-/-/-/-/////////////////////////////////////////////

  /**
   * Test memstore snapshots
   *
   * @throws IOException
   */
  public void testSnapshotting() throws IOException {
    final int snapshotCount = 5;
    // Add some rows, run a snapshot. Do it a few times.
    for (int i = 0; i < snapshotCount; i++) {
      addRows(this.cms);
      runSnapshot(this.cms, true);
      assertEquals("History not being cleared", 0, this.cms.getSnapshot().getCellsCount());
    }
  }

  public void testMultipleVersionsSimple() throws Exception {
    byte[] row = Bytes.toBytes("testRow");
    byte[] family = Bytes.toBytes("testFamily");
    byte[] qf = Bytes.toBytes("testQualifier");
    long[] stamps = { 1, 2, 3 };
    byte[][] values = { Bytes.toBytes("value0"), Bytes.toBytes("value1"),
        Bytes.toBytes("value2") };
    KeyValue key0 = new KeyValue(row, family, qf, stamps[0], values[0]);
    KeyValue key1 = new KeyValue(row, family, qf, stamps[1], values[1]);
    KeyValue key2 = new KeyValue(row, family, qf, stamps[2], values[2]);

    cms.add(key0);
    cms.add(key1);
    cms.add(key2);

    assertTrue("Expected memstore to hold 3 values, actually has " +
        cms.getActive().getCellsCount(), cms.getActive().getCellsCount() == 3);
  }

  /**
   * Test getNextRow from memstore
   *
   * @throws InterruptedException
   */
  public void testGetNextRow() throws Exception {
    addRows(this.cms);
    // Add more versions to make it a little more interesting.
    Thread.sleep(1);
    addRows(this.cms);
    Cell closestToEmpty = this.cms.getNextRow(KeyValue.LOWESTKEY);
    assertTrue(KeyValue.COMPARATOR.compareRows(closestToEmpty,
        new KeyValue(Bytes.toBytes(0), System.currentTimeMillis())) == 0);
    for (int i = 0; i < ROW_COUNT; i++) {
      Cell nr = this.cms.getNextRow(new KeyValue(Bytes.toBytes(i),
          System.currentTimeMillis()));
      if (i + 1 == ROW_COUNT) {
        assertEquals(nr, null);
      } else {
        assertTrue(KeyValue.COMPARATOR.compareRows(nr,
            new KeyValue(Bytes.toBytes(i + 1), System.currentTimeMillis())) == 0);
      }
    }
    //starting from each row, validate results should contain the starting row
    for (int startRowId = 0; startRowId < ROW_COUNT; startRowId++) {
      ScanInfo scanInfo = new ScanInfo(FAMILY, 0, 1, Integer.MAX_VALUE, KeepDeletedCells.FALSE,
          0, this.cms.getComparator());
      ScanType scanType = ScanType.USER_SCAN;
      InternalScanner scanner = new StoreScanner(new Scan(
          Bytes.toBytes(startRowId)), scanInfo, scanType, null,
          cms.getScanners(0));
      List<Cell> results = new ArrayList<Cell>();
      for (int i = 0; scanner.next(results); i++) {
        int rowId = startRowId + i;
        Cell left = results.get(0);
        byte[] row1 = Bytes.toBytes(rowId);
        assertTrue("Row name",
            KeyValue.COMPARATOR.compareRows(left.getRowArray(), left.getRowOffset(),
                (int) left.getRowLength(), row1, 0, row1.length) == 0);
        assertEquals("Count of columns", QUALIFIER_COUNT, results.size());
        List<Cell> row = new ArrayList<Cell>();
        for (Cell kv : results) {
          row.add(kv);
        }
        isExpectedRowWithoutTimestamps(rowId, row);
        // Clear out set.  Otherwise row results accumulate.
        results.clear();
      }
    }
  }

  public void testGet_memstoreAndSnapShot() throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] qf4 = Bytes.toBytes("testqualifier4");
    byte[] qf5 = Bytes.toBytes("testqualifier5");
    byte[] val = Bytes.toBytes("testval");

    //Setting up memstore
    cms.add(new KeyValue(row, fam, qf1, val));
    cms.add(new KeyValue(row, fam, qf2, val));
    cms.add(new KeyValue(row, fam, qf3, val));
    //Creating a snapshot
    cms.snapshot();
    assertEquals(0, cms.getSnapshot().getCellsCount());
    cms.setForceFlush().snapshot();
    assertEquals(3, cms.getSnapshot().getCellsCount());
    //Adding value to "new" memstore
    assertEquals(0, cms.getActive().getCellsCount());
    cms.add(new KeyValue(row, fam, qf4, val));
    cms.add(new KeyValue(row, fam, qf5, val));
    assertEquals(2, cms.getActive().getCellsCount());
  }

  //////////////////////////////////////////////////////////////////////////////
  // Delete tests
  //////////////////////////////////////////////////////////////////////////////
  public void testGetWithDelete() throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier");
    byte[] val = Bytes.toBytes("testval");

    long ts1 = System.nanoTime();
    KeyValue put1 = new KeyValue(row, fam, qf1, ts1, val);
    long ts2 = ts1 + 1;
    KeyValue put2 = new KeyValue(row, fam, qf1, ts2, val);
    long ts3 = ts2 + 1;
    KeyValue put3 = new KeyValue(row, fam, qf1, ts3, val);
    cms.add(put1);
    cms.add(put2);
    cms.add(put3);

    assertEquals(3, cms.getActive().getCellsCount());

    KeyValue del2 = new KeyValue(row, fam, qf1, ts2, KeyValue.Type.Delete, val);
    cms.delete(del2);

    List<Cell> expected = new ArrayList<Cell>();
    expected.add(put3);
    expected.add(del2);
    expected.add(put2);
    expected.add(put1);

    assertEquals(4, cms.getActive().getCellsCount());
    int i = 0;
    for (Cell cell : cms.getActive().getCellSet()) {
      assertEquals(expected.get(i++), cell);
    }
  }

  public void testGetWithDeleteColumn() throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier");
    byte[] val = Bytes.toBytes("testval");

    long ts1 = System.nanoTime();
    KeyValue put1 = new KeyValue(row, fam, qf1, ts1, val);
    long ts2 = ts1 + 1;
    KeyValue put2 = new KeyValue(row, fam, qf1, ts2, val);
    long ts3 = ts2 + 1;
    KeyValue put3 = new KeyValue(row, fam, qf1, ts3, val);
    cms.add(put1);
    cms.add(put2);
    cms.add(put3);

    assertEquals(3, cms.getActive().getCellsCount());

    KeyValue del2 =
        new KeyValue(row, fam, qf1, ts2, KeyValue.Type.DeleteColumn, val);
    cms.delete(del2);

    List<Cell> expected = new ArrayList<Cell>();
    expected.add(put3);
    expected.add(del2);
    expected.add(put2);
    expected.add(put1);

    assertEquals(4, cms.getActive().getCellsCount());
    int i = 0;
    for (Cell cell : cms.getActive().getCellSet()) {
      assertEquals(expected.get(i++), cell);
    }
  }

  public void testGetWithDeleteFamily() throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] val = Bytes.toBytes("testval");
    long ts = System.nanoTime();

    KeyValue put1 = new KeyValue(row, fam, qf1, ts, val);
    KeyValue put2 = new KeyValue(row, fam, qf2, ts, val);
    KeyValue put3 = new KeyValue(row, fam, qf3, ts, val);
    KeyValue put4 = new KeyValue(row, fam, qf3, ts + 1, val);

    cms.add(put1);
    cms.add(put2);
    cms.add(put3);
    cms.add(put4);

    KeyValue del =
        new KeyValue(row, fam, null, ts, KeyValue.Type.DeleteFamily, val);
    cms.delete(del);

    List<Cell> expected = new ArrayList<Cell>();
    expected.add(del);
    expected.add(put1);
    expected.add(put2);
    expected.add(put4);
    expected.add(put3);

    assertEquals(5, cms.getActive().getCellsCount());
    int i = 0;
    for (Cell cell : cms.getActive().getCellSet()) {
      assertEquals(expected.get(i++), cell);
    }
  }

  public void testKeepDeleteInmemstore() {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf = Bytes.toBytes("testqualifier");
    byte[] val = Bytes.toBytes("testval");
    long ts = System.nanoTime();
    cms.add(new KeyValue(row, fam, qf, ts, val));
    KeyValue delete = new KeyValue(row, fam, qf, ts, KeyValue.Type.Delete, val);
    cms.delete(delete);
    assertEquals(2, cms.getActive().getCellsCount());
    assertEquals(delete, cms.getActive().first());
  }

  public void testRetainsDeleteVersion() throws IOException {
    // add a put to memstore
    cms.add(KeyValueTestUtil.create("row1", "fam", "a", 100, "dont-care"));

    // now process a specific delete:
    KeyValue delete = KeyValueTestUtil.create(
        "row1", "fam", "a", 100, KeyValue.Type.Delete, "dont-care");
    cms.delete(delete);

    assertEquals(2, cms.getActive().getCellsCount());
    assertEquals(delete, cms.getActive().first());
  }

  ////////////////////////////////////===================================================
  //Test for timestamps
  ////////////////////////////////////

  public void testRetainsDeleteColumn() throws IOException {
    // add a put to memstore
    cms.add(KeyValueTestUtil.create("row1", "fam", "a", 100, "dont-care"));

    // now process a specific delete:
    KeyValue delete = KeyValueTestUtil.create("row1", "fam", "a", 100,
        KeyValue.Type.DeleteColumn, "dont-care");
    cms.delete(delete);

    assertEquals(2, cms.getActive().getCellsCount());
    assertEquals(delete, cms.getActive().first());
  }

  ////////////////////////////////////
  //Test for upsert with MSLAB
  ////////////////////////////////////

  public void testRetainsDeleteFamily() throws IOException {
    // add a put to memstore
    cms.add(KeyValueTestUtil.create("row1", "fam", "a", 100, "dont-care"));

    // now process a specific delete:
    KeyValue delete = KeyValueTestUtil.create("row1", "fam", "a", 100,
        KeyValue.Type.DeleteFamily, "dont-care");
    cms.delete(delete);

    assertEquals(2, cms.getActive().getCellsCount());
    assertEquals(delete, cms.getActive().first());
  }

  /**
   * Test to ensure correctness when using Memstore with multiple timestamps
   */
  public void testMultipleTimestamps() throws IOException {
    long[] timestamps = new long[] { 20, 10, 5, 1 };
    Scan scan = new Scan();

    for (long timestamp : timestamps)
      addRows(cms, timestamp);

    scan.setTimeRange(0, 2);
    assertTrue(cms.shouldSeek(scan, Long.MIN_VALUE));

    scan.setTimeRange(20, 82);
    assertTrue(cms.shouldSeek(scan, Long.MIN_VALUE));

    scan.setTimeRange(10, 20);
    assertTrue(cms.shouldSeek(scan, Long.MIN_VALUE));

    scan.setTimeRange(8, 12);
    assertTrue(cms.shouldSeek(scan, Long.MIN_VALUE));

    /*This test is not required for correctness but it should pass when
     * timestamp range optimization is on*/
    //scan.setTimeRange(28, 42);
    //assertTrue(!memstore.shouldSeek(scan));
  }

  /**
   * Test a pathological pattern that shows why we can't currently
   * use the MSLAB for upsert workloads. This test inserts data
   * in the following pattern:
   * - row0001 through row1000 (fills up one 2M Chunk)
   * - row0002 through row1001 (fills up another 2M chunk, leaves one reference
   * to the first chunk
   * - row0003 through row1002 (another chunk, another dangling reference)
   * This causes OOME pretty quickly if we use MSLAB for upsert
   * since each 2M chunk is held onto by a single reference.
   */
  public void testUpsertMSLAB() throws Exception {

    int ROW_SIZE = 2048;
    byte[] qualifier = new byte[ROW_SIZE - 4];

    MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
    for (int i = 0; i < 3; i++) {
      System.gc();
    }
    long usageBefore = bean.getHeapMemoryUsage().getUsed();

    long size = 0;
    long ts = 0;

    for (int newValue = 0; newValue < 1000; newValue++) {
      for (int row = newValue; row < newValue + 1000; row++) {
        byte[] rowBytes = Bytes.toBytes(row);
        size += cms.updateColumnValue(rowBytes, FAMILY, qualifier, newValue, ++ts);
      }
    }
    System.out.println("Wrote " + ts + " vals");
    for (int i = 0; i < 3; i++) {
      System.gc();
    }
    long usageAfter = bean.getHeapMemoryUsage().getUsed();
    System.out.println("Memory used: " + (usageAfter - usageBefore)
        + " (heapsize: " + cms.heapSize() +
        " size: " + size + ")");
  }

  ////////////////////////////////////
  // Test for periodic memstore flushes
  // based on time of oldest edit
  ////////////////////////////////////

  /**
   * Add keyvalues with a fixed memstoreTs, and checks that memstore size is decreased
   * as older keyvalues are deleted from the memstore.
   *
   * @throws Exception
   */
  public void testUpsertMemstoreSize() throws Exception {
    long oldSize = cms.size();

    List<Cell> l = new ArrayList<Cell>();
    KeyValue kv1 = KeyValueTestUtil.create("r", "f", "q", 100, "v");
    KeyValue kv2 = KeyValueTestUtil.create("r", "f", "q", 101, "v");
    KeyValue kv3 = KeyValueTestUtil.create("r", "f", "q", 102, "v");

    kv1.setSequenceId(1);
    kv2.setSequenceId(1);
    kv3.setSequenceId(1);
    l.add(kv1);
    l.add(kv2);
    l.add(kv3);

    this.cms.upsert(l, 2);// readpoint is 2
    long newSize = this.cms.size();
    assert (newSize > oldSize);
    //The kv1 should be removed.
    assert (cms.getActive().getCellsCount() == 2);

    KeyValue kv4 = KeyValueTestUtil.create("r", "f", "q", 104, "v");
    kv4.setSequenceId(1);
    l.clear();
    l.add(kv4);
    this.cms.upsert(l, 3);
    assertEquals(newSize, this.cms.size());
    //The kv2 should be removed.
    assert (cms.getActive().getCellsCount() == 2);
    //this.memstore = null;
  }

  /**
   * Tests that the timeOfOldestEdit is updated correctly for the
   * various edit operations in memstore.
   *
   * @throws Exception
   */
  public void testUpdateToTimeOfOldestEdit() throws Exception {
    try {
      EnvironmentEdgeForMemstoreTest edge = new EnvironmentEdgeForMemstoreTest();
      EnvironmentEdgeManager.injectEdge(edge);
      long t = cms.timeOfOldestEdit();
      assertEquals(t, Long.MAX_VALUE);

      // test the case that the timeOfOldestEdit is updated after a KV add
      cms.add(KeyValueTestUtil.create("r", "f", "q", 100, "v"));
      t = cms.timeOfOldestEdit();
      assertTrue(t == 1234);
      // snapshot() after setForceFlush() will reset timeOfOldestEdit. The method will also assert
      // the value is reset to Long.MAX_VALUE

      //           t = runSnapshot(compacmemstore, false);
      t = runSnapshot(cms, true);

      // test the case that the timeOfOldestEdit is updated after a KV delete
      cms.delete(KeyValueTestUtil.create("r", "f", "q", 100, "v"));
      t = cms.timeOfOldestEdit();
      assertTrue(t == 1234);

      t = runSnapshot(cms, true);

      // test the case that the timeOfOldestEdit is updated after a KV upsert
      List<Cell> l = new ArrayList<Cell>();
      KeyValue kv1 = KeyValueTestUtil.create("r", "f", "q", 100, "v");
      kv1.setSequenceId(100);
      l.add(kv1);
      cms.upsert(l, 1000);
      t = cms.timeOfOldestEdit();
      assertTrue(t == 1234);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  /**
   * Tests the HRegion.shouldFlush method - adds an edit in the memstore
   * and checks that shouldFlush returns true, and another where it disables
   * the periodic flush functionality and tests whether shouldFlush returns
   * false.
   *
   * @throws Exception
   */
  public void testShouldFlush() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 1000);
    checkShouldFlush(conf, true);
    // test disable flush
    conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 0);
    checkShouldFlush(conf, false);
  }

  private void checkShouldFlush(Configuration conf, boolean expected) throws Exception {
    try {
      EnvironmentEdgeForMemstoreTest edge = new EnvironmentEdgeForMemstoreTest();
      EnvironmentEdgeManager.injectEdge(edge);
      HBaseTestingUtility hbaseUtility = HBaseTestingUtility.createLocalHTU(conf);
      HRegion region = hbaseUtility.createTestRegion("foobar", new HColumnDescriptor("foo"));

      List<Store> stores = region.getStores();
      assertTrue(stores.size() == 1);

      Store s = stores.iterator().next();
      edge.setCurrentTimeMillis(1234);
      s.add(KeyValueTestUtil.create("r", "f", "q", 100, "v"));
      edge.setCurrentTimeMillis(1234 + 100);
      assertTrue(!region.shouldFlush());
      edge.setCurrentTimeMillis(1234 + 10000);
      assertTrue(region.shouldFlush() == expected);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  /**
   * Adds {@link #ROW_COUNT} rows and {@link #QUALIFIER_COUNT}
   *
   * @param hmc Instance to add rows to.
   * @return How many rows we added.
   * @throws IOException
   */
  private int addRows(final AbstractMemStore hmc) {
    return addRows(hmc, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Adds {@link #ROW_COUNT} rows and {@link #QUALIFIER_COUNT}
   *
   * @param hmc Instance to add rows to.
   * @return How many rows we added.
   * @throws IOException
   */
  private int addRows(final AbstractMemStore hmc, final long ts) {
    for (int i = 0; i < ROW_COUNT; i++) {
      long timestamp = ts == HConstants.LATEST_TIMESTAMP ?
          System.currentTimeMillis() : ts;
      for (int ii = 0; ii < QUALIFIER_COUNT; ii++) {
        byte[] row = Bytes.toBytes(i);
        byte[] qf = makeQualifier(i, ii);
        hmc.add(new KeyValue(row, FAMILY, qf, timestamp, qf));
      }
    }
    return ROW_COUNT;
  }

  private long runSnapshot(final CompactedMemStore hmc, boolean useForce)
      throws IOException {
    // Save off old state.
    long oldHistorySize = hmc.getSnapshot().getSize();
    long prevTimeStamp = hmc.timeOfOldestEdit();
    if (useForce) hmc.setForceFlush();
    hmc.snapshot();
    MemStoreSnapshot snapshot = hmc.snapshot();
    if (useForce) {
      // Make some assertions about what just happened.
      assertTrue("History size has not increased", oldHistorySize < snapshot.getSize());
      long t = hmc.timeOfOldestEdit();
      assertTrue("Time of oldest edit is not Long.MAX_VALUE", t == Long.MAX_VALUE);
      hmc.clearSnapshot(snapshot.getId());
    } else {
      long t = hmc.timeOfOldestEdit();
      assertTrue("Time of oldest edit didn't remain the same", t == prevTimeStamp);
    }
    return prevTimeStamp;
  }

  private void isExpectedRowWithoutTimestamps(final int rowIndex,
      List<Cell> kvs) {
    int i = 0;
    for (Cell kv : kvs) {
      byte[] expectedColname = makeQualifier(rowIndex, i++);
      assertTrue("Column name", CellUtil.matchingQualifier(kv, expectedColname));
      // Value is column name as bytes.  Usually result is
      // 100 bytes in size at least. This is the default size
      // for BytesWriteable.  For comparison, convert bytes to
      // String and trim to remove trailing null bytes.
      assertTrue("Content", CellUtil.matchingValue(kv, expectedColname));
    }
  }

  @Test
  public void testPuttingBackChunksAfterFlushing() throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] qf4 = Bytes.toBytes("testqualifier4");
    byte[] qf5 = Bytes.toBytes("testqualifier5");
    byte[] val = Bytes.toBytes("testval");

    // Setting up memstore
    cms.add(new KeyValue(row, fam, qf1, val));
    cms.add(new KeyValue(row, fam, qf2, val));
    cms.add(new KeyValue(row, fam, qf3, val));

    // Creating a snapshot
    cms.setForceFlush();
    MemStoreSnapshot snapshot = cms.snapshot();
    assertEquals(3, cms.getSnapshot().getCellsCount());

    // Adding value to "new" memstore
    assertEquals(0, cms.getActive().getCellsCount());
    cms.add(new KeyValue(row, fam, qf4, val));
    cms.add(new KeyValue(row, fam, qf5, val));
    assertEquals(2, cms.getActive().getCellsCount());
    cms.clearSnapshot(snapshot.getId());

    int chunkCount = chunkPool.getPoolSize();
    assertTrue(chunkCount > 0);

  }

  @Test
  public void testPuttingBackChunksWithOpeningScanner()
      throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] qf4 = Bytes.toBytes("testqualifier4");
    byte[] qf5 = Bytes.toBytes("testqualifier5");
    byte[] qf6 = Bytes.toBytes("testqualifier6");
    byte[] qf7 = Bytes.toBytes("testqualifier7");
    byte[] val = Bytes.toBytes("testval");

    // Setting up memstore
    cms.add(new KeyValue(row, fam, qf1, val));
    cms.add(new KeyValue(row, fam, qf2, val));
    cms.add(new KeyValue(row, fam, qf3, val));

    // Creating a snapshot
    cms.setForceFlush();
    MemStoreSnapshot snapshot = cms.snapshot();
    assertEquals(3, cms.getSnapshot().getCellsCount());

    // Adding value to "new" memstore
    assertEquals(0, cms.getActive().getCellsCount());
    cms.add(new KeyValue(row, fam, qf4, val));
    cms.add(new KeyValue(row, fam, qf5, val));
    assertEquals(2, cms.getActive().getCellsCount());

    // opening scanner before clear the snapshot
    List<KeyValueScanner> scanners = cms.getScanners(0);
    // Shouldn't putting back the chunks to pool,since some scanners are opening
    // based on their data
    cms.clearSnapshot(snapshot.getId());

    assertTrue(chunkPool.getPoolSize() == 0);

    // Chunks will be put back to pool after close scanners;
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    assertTrue(chunkPool.getPoolSize() > 0);

    // clear chunks
    chunkPool.clearChunks();

    // Creating another snapshot
    cms.setForceFlush();
    snapshot = cms.snapshot();
    // Adding more value
    cms.add(new KeyValue(row, fam, qf6, val));
    cms.add(new KeyValue(row, fam, qf7, val));
    // opening scanners
    scanners = cms.getScanners(0);
    // close scanners before clear the snapshot
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    // Since no opening scanner, the chunks of snapshot should be put back to
    // pool
    cms.clearSnapshot(snapshot.getId());
    assertTrue(chunkPool.getPoolSize() > 0);
  }

  @Test
  public void testPuttingBackChunksWithOpeningPipelineScanner()
      throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] val = Bytes.toBytes("testval");

    // Setting up memstore
    cms.add(new KeyValue(row, fam, qf1, 1, val));
    cms.add(new KeyValue(row, fam, qf2, 1, val));
    cms.add(new KeyValue(row, fam, qf3, 1, val));

    // Creating a pipeline
    cms.disableCompaction();
    cms.snapshot();

    // Adding value to "new" memstore
    assertEquals(0, cms.getActive().getCellsCount());
    cms.add(new KeyValue(row, fam, qf1, 2, val));
    cms.add(new KeyValue(row, fam, qf2, 2, val));
    assertEquals(2, cms.getActive().getCellsCount());

    // pipeline bucket 2
    cms.snapshot();
    // opening scanner before force flushing
    List<KeyValueScanner> scanners = cms.getScanners(0);
    // Shouldn't putting back the chunks to pool,since some scanners are opening
    // based on their data
    cms.enableCompaction();
    // trigger compaction
    cms.snapshot();

    // Adding value to "new" memstore
    assertEquals(0, cms.getActive().getCellsCount());
    cms.add(new KeyValue(row, fam, qf3, 3, val));
    cms.add(new KeyValue(row, fam, qf2, 3, val));
    cms.add(new KeyValue(row, fam, qf1, 3, val));
    assertEquals(3, cms.getActive().getCellsCount());

    while (cms.isMemstoreCompaction()) {
      Threads.sleep(10);
    }

    assertTrue(chunkPool.getPoolSize() == 0);

    // Chunks will be put back to pool after close scanners;
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    assertTrue(chunkPool.getPoolSize() > 0);

    // clear chunks
    chunkPool.clearChunks();

    // Creating another snapshot
    cms.setForceFlush();
    MemStoreSnapshot snapshot = cms.snapshot();
    cms.clearSnapshot(snapshot.getId());
    cms.setForceFlush();
    snapshot = cms.snapshot();
    // Adding more value
    cms.add(new KeyValue(row, fam, qf2, 4, val));
    cms.add(new KeyValue(row, fam, qf3, 4, val));
    // opening scanners
    scanners = cms.getScanners(0);
    // close scanners before clear the snapshot
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    // Since no opening scanner, the chunks of snapshot should be put back to
    // pool
    cms.clearSnapshot(snapshot.getId());
    assertTrue(chunkPool.getPoolSize() > 0);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction tests
  //////////////////////////////////////////////////////////////////////////////
  public void testCompaction1Bucket() throws IOException {

    String[] keys1 = { "A", "A", "B", "C" }; //A1, A2, B3, C4

    // test 1 bucket
    addRowsByKeys(cms, keys1);
    assertEquals(704, region.getMemstoreTotalSize());

    long size = cms.getFlushableSize();
    cms.snapshot(); // push keys to pipeline and compact
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher thread
    while (cms.isMemstoreCompaction()) {
      Threads.sleep(10);
    }
    assertEquals(0, cms.getSnapshot().getCellsCount());
    assertEquals(528, region.getMemstoreTotalSize());

    cms.setForceFlush();
    size = cms.getFlushableSize();
    MemStoreSnapshot snapshot = cms.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    MemStoreSegment s = cms.getSnapshot();
    SortedSet<Cell> ss = s.getCellSet();
    assertEquals(3, s.getCellsCount());
    assertEquals(0, region.getMemstoreTotalSize());

    cms.clearSnapshot(snapshot.getId());
  }

  public void testCompaction2Buckets() throws IOException {

    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };

    addRowsByKeys(cms, keys1);
    assertEquals(704, region.getMemstoreTotalSize());

    long size = cms.getFlushableSize();
    cms.snapshot(); // push keys to pipeline and compact
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher thread
    while (cms.isMemstoreCompaction()) {
      Threads.sleep(10);
    }
    assertEquals(0, cms.getSnapshot().getCellsCount());
    assertEquals(528, region.getMemstoreTotalSize());

    addRowsByKeys(cms, keys2);
    assertEquals(1056, region.getMemstoreTotalSize());

    size = cms.getFlushableSize();
    cms.snapshot(); // push keys to pipeline and compact
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher thread
    while (cms.isMemstoreCompaction()) {
      Threads.sleep(10);
    }
    assertEquals(0, cms.getSnapshot().getCellsCount());
    assertEquals(704, region.getMemstoreTotalSize());

    cms.setForceFlush();
    size = cms.getFlushableSize();
    MemStoreSnapshot snapshot = cms.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    MemStoreSegment s = cms.getSnapshot();
    SortedSet<Cell> ss = s.getCellSet();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, region.getMemstoreTotalSize());

    cms.clearSnapshot(snapshot.getId());
  }

  public void testCompaction3Buckets() throws IOException {

    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };
    String[] keys3 = { "D", "B", "B" };

    addRowsByKeys(cms, keys1);
    assertEquals(704, region.getMemstoreSize());

    long size = cms.getFlushableSize();
    cms.snapshot(); // push keys to pipeline and compact
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher thread
    while (cms.isMemstoreCompaction()) {
      Threads.sleep(10);
    }
    assertEquals(0, cms.getSnapshot().getCellsCount());
    assertEquals(0, region.getMemstoreSize());
    assertEquals(528, region.getMemstoreTotalSize());

    addRowsByKeys(cms, keys2);
    assertEquals(528, region.getMemstoreSize());
    assertEquals(1056, region.getMemstoreTotalSize());

    cms.disableCompaction();
    size = cms.getFlushableSize();
    cms.snapshot(); // push keys to pipeline without compaction
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher thread
    assertEquals(0, cms.getSnapshot().getCellsCount());
    assertEquals(0, region.getMemstoreSize());
    assertEquals(1056, region.getMemstoreTotalSize());

    addRowsByKeys(cms, keys3);
    assertEquals(528, region.getMemstoreSize());
    assertEquals(1584, region.getMemstoreTotalSize());

    cms.enableCompaction();
    size = cms.getFlushableSize();
    cms.snapshot(); // push keys to pipeline and compact
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher thread
    while (cms.isMemstoreCompaction()) {
      Threads.sleep(10);
    }
    assertEquals(0, cms.getSnapshot().getCellsCount());
    assertEquals(0, region.getMemstoreSize());
    assertEquals(704, region.getMemstoreTotalSize());

    cms.setForceFlush();
    size = cms.getFlushableSize();
    MemStoreSnapshot snapshot = cms.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    MemStoreSegment s = cms.getSnapshot();
    SortedSet<Cell> ss = s.getCellSet();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, region.getMemstoreSize());
    assertEquals(0, region.getMemstoreTotalSize());

    cms.clearSnapshot(snapshot.getId());
  }

  private void addRowsByKeys(final AbstractMemStore hmc, String[] keys) {
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf = Bytes.toBytes("testqualifier");
    for (int i = 0; i < keys.length; i++) {
      long timestamp = System.currentTimeMillis();
      Threads.sleep(1); // to make sure each kv gets a different ts
      byte[] row = Bytes.toBytes(keys[i]);
      byte[] val = Bytes.toBytes(keys[i] + i);
      KeyValue kv = new KeyValue(row, fam, qf, timestamp, val);
      hmc.add(kv);
      LOG.debug("added kv: " + kv.getKeyString() + ", timestamp" + kv.getTimestamp());
      long size = AbstractMemStore.heapSizeChange(kv, true);
      region.addAndGetGlobalMemstoreSize(size);
    }
  }

  private static class ReadOwnWritesTester extends Thread {
    static final int NUM_TRIES = 1000;

    final byte[] row;

    final byte[] f = Bytes.toBytes("family");
    final byte[] q1 = Bytes.toBytes("q1");

    final MultiVersionConsistencyControl mvcc;
    final CompactedMemStore compmemstore;
    final AtomicLong startSeqNum;

    AtomicReference<Throwable> caughtException;

    public ReadOwnWritesTester(int id,
        CompactedMemStore memstore,
        MultiVersionConsistencyControl mvcc,
        AtomicReference<Throwable> caughtException,
        AtomicLong startSeqNum) {
      this.mvcc = mvcc;
      this.compmemstore = memstore;
      this.caughtException = caughtException;
      row = Bytes.toBytes(id);
      this.startSeqNum = startSeqNum;
    }

    public void run() {
      try {
        internalRun();
      } catch (Throwable t) {
        caughtException.compareAndSet(null, t);
      }
    }

    private void internalRun() throws IOException {
      for (long i = 0; i < NUM_TRIES && caughtException.get() == null; i++) {
        MultiVersionConsistencyControl.WriteEntry w =
            mvcc.beginMemstoreInsert();

        // Insert the sequence value (i)
        byte[] v = Bytes.toBytes(i);

        KeyValue kv = new KeyValue(row, f, q1, i, v);
        kv.setSequenceId(w.getWriteNumber());
        compmemstore.add(kv);
        mvcc.completeMemstoreInsert(w);

        // Assert that we can read back
        KeyValueScanner s = this.compmemstore.getScanners(mvcc.memstoreReadPoint()).get(0);
        s.seek(kv);

        Cell ret = s.next();
        assertNotNull("Didnt find own write at all", ret);
        assertEquals("Didnt read own writes",
            kv.getTimestamp(), ret.getTimestamp());
      }
    }
  }

  private class EnvironmentEdgeForMemstoreTest implements EnvironmentEdge {
    long t = 1234;

    @Override
    public long currentTime() {
            return t;
        }
      public void setCurrentTimeMillis(long t) {
        this.t = t;
      }
    }

}
