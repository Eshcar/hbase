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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * compacted memstore test case
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestCompactingToCellChunkMapMemStore extends TestCompactingMemStore {

  private static final Log LOG = LogFactory.getLog(TestCompactingToCellChunkMapMemStore.class);
  //private static MemStoreChunkPool chunkPool;
  //private HRegion region;
  //private RegionServicesForStores regionServicesForStores;
  //private HStore store;

  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////

  @Override public void tearDown() throws Exception {
    chunkPool.clearChunks();
  }

  @Override public void setUp() throws Exception {
    compactingSetUp();
    this.memstore =
        new CompactingMemStore(HBaseConfiguration.create(), CellComparator.COMPARATOR, store,
            regionServicesForStores, CompactingMemStore.Type.COMPACT_TO_CHUNK_MAP);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction tests
  //////////////////////////////////////////////////////////////////////////////
  public void testCompaction1Bucket() throws IOException {
    int counter = 0;
    String[] keys1 = { "A", "A", "B", "C" }; //A1, A2, B3, C4

    // test 1 bucket
    addRowsByKeys(memstore, keys1);
    assertEquals(704, regionServicesForStores.getGlobalMemstoreTotalSize());
    assertEquals(4, memstore.getActive().getCellsCount());
    long size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(440, regionServicesForStores.getGlobalMemstoreTotalSize());
    for ( Segment s : memstore.getListOfSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(3, counter);
    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(3, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getGlobalMemstoreTotalSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  public void testCompaction2Buckets() throws IOException {

    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };

    addRowsByKeys(memstore, keys1);
    assertEquals(704, regionServicesForStores.getGlobalMemstoreTotalSize());
    long size = memstore.getFlushableSize();

//    assertTrue(
//        "\n\n<<< This is the active size with 4 keys - " + memstore.getActive().getSize()
//            + ". This is the memstore flushable size - " + size + "\n",false);

    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(1000);
    }
    int counter = 0;
    for ( Segment s : memstore.getListOfSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(3,counter);
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(440, regionServicesForStores.getGlobalMemstoreTotalSize());

    addRowsByKeys(memstore, keys2);
    assertEquals(968, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    int i = 0;
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
      if (i > 10000000) {
        ((CompactingMemStore) memstore).debug();
        assertTrue("\n\n<<< Infinite loop! :( \n", false);
      }
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    counter = 0;
    for ( Segment s : memstore.getListOfSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(4,counter);
    assertEquals(592, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getGlobalMemstoreTotalSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  public void testCompaction3Buckets() throws IOException {

    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };
    String[] keys3 = { "D", "B", "B" };

    addRowsByKeys(memstore, keys1);
    assertEquals(704, region.getMemstoreSize());

    long size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact

    String tstStr = "\n\nFlushable size after first flush in memory:" + size + ". Is MemmStore in compaction?:"
        + ((CompactingMemStore) memstore).isMemStoreFlushingInMemory();
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(440, regionServicesForStores.getGlobalMemstoreTotalSize());

    addRowsByKeys(memstore, keys2);

    tstStr += " After adding second part of the keys. Memstore size: " +
        region.getMemstoreSize() + ", Memstore Total Size: " +
        regionServicesForStores.getGlobalMemstoreTotalSize() + "\n\n";

    assertEquals(968, regionServicesForStores.getGlobalMemstoreTotalSize());

    ((CompactingMemStore) memstore).disableCompaction();
    size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline without compaction
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(968, regionServicesForStores.getGlobalMemstoreTotalSize());

    addRowsByKeys(memstore, keys3);
    assertEquals(1496, regionServicesForStores.getGlobalMemstoreTotalSize());

    ((CompactingMemStore) memstore).enableCompaction();
    size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(592, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getGlobalMemstoreTotalSize());

    memstore.clearSnapshot(snapshot.getId());

    //assertTrue(tstStr, false);
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
      regionServicesForStores.addAndGetGlobalMemstoreSize(size);
    }
  }

  private class EnvironmentEdgeForMemstoreTest implements EnvironmentEdge {
    long t = 1234;

    @Override public long currentTime() {
      return t;
    }

    public void setCurrentTimeMillis(long t) {
      this.t = t;
    }
  }

}
