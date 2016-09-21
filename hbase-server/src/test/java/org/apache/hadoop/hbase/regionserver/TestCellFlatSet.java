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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import static org.junit.Assert.assertTrue;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCellFlatSet extends TestCase {

  private static final int NUM_OF_CELLS = 4;

  /* 16645 start */
  private Cell ascCells[];
  private CellArrayMap ascCbOnHeap;
  private Cell descCells[];
  private CellArrayMap descCbOnHeap;
  private KeyValue innerCell;
  private KeyValue outerCell;
  /* 16645 end */

  private Cell cells[];
  private CellArrayMap cbOnHeap;

  private final static Configuration conf = new Configuration();
  private HeapMemStoreLAB mslab;


  protected void setUp() throws Exception {
    super.setUp();

    // create array of Cells to bass to the CellFlatMap under CellSet
    final byte[] one = Bytes.toBytes(15);
    final byte[] two = Bytes.toBytes(25);
    final byte[] three = Bytes.toBytes(35);
    final byte[] four = Bytes.toBytes(45);

    final byte[] f = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    final byte[] v = Bytes.toBytes(4);

    final KeyValue kv1 = new KeyValue(one, f, q, 10, v);
    final KeyValue kv2 = new KeyValue(two, f, q, 20, v);
    final KeyValue kv3 = new KeyValue(three, f, q, 30, v);
    final KeyValue kv4 = new KeyValue(four, f, q, 40, v);

    /* 16645 start */
    innerCell = new KeyValue(Bytes.toBytes(10), f, q, 10, v);
    outerCell = new KeyValue(Bytes.toBytes(50), f, q, 10, v);
    ascCells = new Cell[] {kv1,kv2,kv3,kv4};
    ascCbOnHeap = new CellArrayMap(CellComparator.COMPARATOR,ascCells,0,NUM_OF_CELLS,false);
    descCells = new Cell[] {kv4,kv3,kv2,kv1};
    descCbOnHeap = new CellArrayMap(CellComparator.COMPARATOR,descCells,0,NUM_OF_CELLS,true);
    /* 16645 end */

    cells = new Cell[] {kv1,kv2,kv3,kv4};
    cbOnHeap = new CellArrayMap(CellComparator.COMPARATOR,cells,0,NUM_OF_CELLS,false);

    conf.setBoolean(SegmentFactory.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreChunkPool.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    MemStoreChunkPool.chunkPoolDisabled = false;
    mslab = new HeapMemStoreLAB(conf);
  }

  /* Create and test CellSet based on CellArrayMap */
  public void testCellBlocksOnHeap() throws Exception {
    CellSet cs = new CellSet(cbOnHeap);
    testCellBlocks(cs);
    testIterators(cs);
  }

  /* 16645 start */

  @Test
  public void testAsc() throws Exception {
    CellSet ascCs = new CellSet(ascCbOnHeap);
    assertEquals(NUM_OF_CELLS, ascCs.size());
    testSubSet(ascCs);
    //assert false;
  }

  @Test
  public void testDesc() throws Exception {
    CellSet descCs = new CellSet(descCbOnHeap);
    assertEquals(NUM_OF_CELLS, descCs.size());
    testSubSet(descCs);
  }

  private void testSubSet(CellSet cs) throws Exception {
    for (int i = 0; i != ascCells.length; ++i) {
      NavigableSet<Cell> excludeTail = cs.tailSet(ascCells[i], false);
      NavigableSet<Cell> includeTail = cs.tailSet(ascCells[i], true);
      assertEquals(ascCells.length - 1 - i, excludeTail.size());
      assertEquals(ascCells.length - i, includeTail.size());
      Iterator<Cell> excludeIter = excludeTail.iterator();
      Iterator<Cell> includeIter = includeTail.iterator();
      for (int j = 1 + i; j != ascCells.length; ++j) {
        assertEquals(true, CellUtil.equals(excludeIter.next(), ascCells[j]));
      }
      for (int j = i; j != ascCells.length; ++j) {
        assertEquals(true, CellUtil.equals(includeIter.next(), ascCells[j]));
      }
    }
    assertEquals(NUM_OF_CELLS, cs.tailSet(innerCell, false).size());
    assertEquals(0, cs.tailSet(outerCell, false).size());
    for (int i = 0; i != ascCells.length; ++i) {
      NavigableSet<Cell> excludeHead = cs.headSet(ascCells[i], false);
      NavigableSet<Cell> includeHead = cs.headSet(ascCells[i], true);
      assertEquals(i, excludeHead.size());
      assertEquals(i + 1, includeHead.size());
      Iterator<Cell> excludeIter = excludeHead.iterator();
      Iterator<Cell> includeIter = includeHead.iterator();
      for (int j = 0; j != i; ++j) {
        assertEquals(true, CellUtil.equals(excludeIter.next(), ascCells[j]));
      }
      for (int j = 0; j != i + 1; ++j) {
        assertEquals(true, CellUtil.equals(includeIter.next(), ascCells[j]));
      }
    }
    assertEquals(0, cs.headSet(innerCell, false).size());
    assertEquals(NUM_OF_CELLS, cs.headSet(outerCell, false).size());
  }
  /* 16645 end */

  /* Generic basic test for immutable CellSet */
  private void testCellBlocks(CellSet cs) throws Exception {
    final byte[] oneAndHalf = Bytes.toBytes(20);
    final byte[] f = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    final byte[] v = Bytes.toBytes(4);
    final KeyValue outerCell = new KeyValue(oneAndHalf, f, q, 10, v);

    assertEquals(NUM_OF_CELLS, cs.size());          // check size
    assertFalse(cs.contains(outerCell));            // check outer cell

    assertTrue(cs.contains(cells[0]));              // check existence of the first
    Cell first = cs.first();
    assertTrue(cells[0].equals(first));

    assertTrue(cs.contains(cells[NUM_OF_CELLS - 1]));  // check last
    Cell last = cs.last();
    assertTrue(cells[NUM_OF_CELLS - 1].equals(last));

    SortedSet<Cell> tail = cs.tailSet(cells[1]);    // check tail abd head sizes
    assertEquals(NUM_OF_CELLS - 1, tail.size());
    SortedSet<Cell> head = cs.headSet(cells[1]);
    assertEquals(1, head.size());

    SortedSet<Cell> tailOuter = cs.tailSet(outerCell);  // check tail starting from outer cell
    assertEquals(NUM_OF_CELLS - 1, tailOuter.size());

    Cell tailFirst = tail.first();
    assertTrue(cells[1].equals(tailFirst));
    Cell tailLast = tail.last();
    assertTrue(cells[NUM_OF_CELLS - 1].equals(tailLast));

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
}
