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

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.SortedSet;
import static org.junit.Assert.assertTrue;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCellBlocksSet extends TestCase {

  private static final int NUM_OF_CELLS = 3;

  private Cell cells[];
  private CellBlockOnHeap cbOnHeap;
  private CellBlockOffHeap cbOffHeap;

  private final static Configuration conf = new Configuration();
  private HeapMemStoreLAB mslab;



  protected void setUp() throws Exception {
    super.setUp();

    // create array of Cells to bass to the CellBlock under CellSet
    final byte[] one = Bytes.toBytes(1);
    final byte[] two = Bytes.toBytes(2);
    final byte[] three = Bytes.toBytes(3);
    final byte[] f = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    final byte[] v = Bytes.toBytes(4);

    final KeyValue kv1 = new KeyValue(one, f, q, 10, v);
    final KeyValue kv2 = new KeyValue(two, f, q, 20, v);
    final KeyValue kv3 = new KeyValue(three, f, q, 30, v);

    cells = new Cell[] {kv1,kv2,kv3};
    cbOnHeap = new CellBlockOnHeap(CellComparator.COMPARATOR,cells,0,NUM_OF_CELLS,false);

    conf.setBoolean(SegmentFactory.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreChunkPool.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    MemStoreChunkPool.chunkPoolDisabled = false;
    mslab = new HeapMemStoreLAB(conf);

    HeapMemStoreLAB.Chunk[] c = shallowCellsToBuffer(kv1, kv2, kv3);
    int chunkSize = conf.getInt(HeapMemStoreLAB.CHUNK_SIZE_KEY, HeapMemStoreLAB.CHUNK_SIZE_DEFAULT);
    cbOffHeap = new CellBlockOffHeap(CellComparator.COMPARATOR, mslab,
        c, 0, NUM_OF_CELLS, chunkSize, false);
  }

  /* Create and test CellSet based on CellBlockOnHeap */
  public void testCellBlocksOnHeap() throws Exception {
    CellSet cs = new CellSet(cbOnHeap);
    testCellBlocks(cs);
    testIterators(cs);
  }

  /* Create and test CellSet based on CellBlockOffHeap */
  public void testCellBlocksOffHeap() throws Exception {
    CellSet cs = new CellSet(cbOffHeap);
    testCellBlocks(cs);
    testIterators(cs);
  }

  /* Generic basic test for immutable CellSet */
  private void testCellBlocks(CellSet cs) throws Exception {
    assertEquals(NUM_OF_CELLS, cs.size());    // check size

    assertTrue(cs.contains(cells[0]));        // check first
    Cell first = cs.first();
    assertTrue(cells[0].equals(first));

    assertTrue(cs.contains(cells[NUM_OF_CELLS - 1]));  // check last
    Cell last = cs.last();
    assertTrue(cells[NUM_OF_CELLS - 1].equals(last));

    SortedSet<Cell> tail = cs.tailSet(cells[1]);  // check tail abd head sizes
    assertEquals(2, tail.size());
    SortedSet<Cell> head = cs.headSet(cells[1]);
    assertEquals(1, head.size());

    Cell tailFirst = tail.first();
    assertTrue(cells[1].equals(tailFirst));
    Cell tailLast = tail.last();
    assertTrue(cells[2].equals(tailLast));

    Cell headFirst = head.first();
    assertTrue(cells[0].equals(headFirst));
    Cell headLast = head.last();
    assertTrue(cells[0].equals(headLast));
  }

  /* Generic iterators test for immutable CellSet */
  private void testIterators(CellSet cs) throws Exception {

    // Assert that we have NUM_OF_CELLS values and that they are in order
    int count = 0;
    for (Cell kv: cs) {
      assertEquals("\n\n-------------------------------------------------------------------\n"
              + "Comparing iteration number " + (count + 1) + " the returned cell: " + kv
              + ", the first Cell in the CellBlocksMap: " + cells[count]
              + ", and the same transformed to String: " + cells[count].toString()
              + "\n-------------------------------------------------------------------\n",
              cells[count], kv);
      count++;
    }
    assertEquals(NUM_OF_CELLS, count);

    // Test descending iterator
    count = 0;
    for (Iterator<Cell> i = cs.descendingIterator(); i.hasNext();) {
      Cell kv = i.next();
      assertEquals(cells[NUM_OF_CELLS - (count + 1)], kv);
      count++;
    }
    assertEquals(NUM_OF_CELLS, count);
  }

  /* Create byte array holding shallow Cells referencing to the deep Cells data */
  private HeapMemStoreLAB.Chunk[] shallowCellsToBuffer(Cell kv1, Cell kv2, Cell kv3) {
    HeapMemStoreLAB.Chunk chunkD = mslab.allocateChunk();
    HeapMemStoreLAB.Chunk chunkS = mslab.allocateChunk();
    HeapMemStoreLAB.Chunk result[] = {chunkS};

    byte[] deepBuffer = chunkD.getData();
    byte[] shallowBuffer = chunkS.getData();
    int offset = 0;
    int pos = offset;

//    assertTrue("\n\n<<<<< Preparing for CellBlockOffHeap, shallow chunk: " + chunkS
//        + " has index: " + chunkS.getId() + " \n\n", false);

    KeyValueUtil.appendToByteArray(kv1, deepBuffer, offset);      // write deep cell data

//    assertTrue("\n\n<<<<< Preparing for CellBlockOffHeap, shallow chunk: " + chunkS
//        + " has index: " + chunkS.getId() + ", deep chunk: " + chunkD
//        + " has index: " + chunkD.getId() + "\n\n", false);

    pos = Bytes.putInt(shallowBuffer, pos, chunkD.getId());           // write deep chunk index
    pos = Bytes.putInt(shallowBuffer, pos, offset);                   // offset
    pos = Bytes.putInt(shallowBuffer, pos, KeyValueUtil.length(kv1)); // length
    offset += KeyValueUtil.length(kv1);

    KeyValueUtil.appendToByteArray(kv2, deepBuffer, offset);          // write deep cell data
    pos = Bytes.putInt(shallowBuffer, pos, chunkD.getId());           // deep chunk index
    pos = Bytes.putInt(shallowBuffer, pos, offset);                   // offset
    pos = Bytes.putInt(shallowBuffer, pos, KeyValueUtil.length(kv2)); // length
    offset += KeyValueUtil.length(kv2);

    KeyValueUtil.appendToByteArray(kv3, deepBuffer, offset);          // write deep cell data
    pos = Bytes.putInt(shallowBuffer, pos, chunkD.getId());           // deep chunk index
    pos = Bytes.putInt(shallowBuffer, pos, offset);                   // offset
    pos = Bytes.putInt(shallowBuffer, pos, KeyValueUtil.length(kv3)); // length

    return result;
  }
}
