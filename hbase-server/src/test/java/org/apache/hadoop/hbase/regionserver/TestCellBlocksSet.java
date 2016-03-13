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
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.SortedSet;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCellBlocksSet extends TestCase {

  private static final int NUM_OF_CELLS = 3;

  private byte[] deepBuffer;      // deep cell data
  private byte[] shallowBuffer;   // shallow cell data

  private Cell cells[];
  private CellBlocksOnHeap cbMap;
  private CellSet cbSet;

  protected void setUp() throws Exception {
    super.setUp();

    // create array of Cells to bass to the CellBlocks under CellSet
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
    cbMap = new CellBlocksOnHeap(CellComparator.COMPARATOR,cells,0,NUM_OF_CELLS,false);
    cbSet = new CellSet(cbMap);

    /*---------------- shallow cells to bytes start ----------------*/
    deepBuffer = new byte[128];
    shallowBuffer = new byte[64];
    int offset = 0;
    int pos = offset;

    KeyValueUtil.appendToByteArray(kv1, deepBuffer, offset);
    pos = Bytes.putInt(shallowBuffer, pos, 0);                        // chunk index
    pos = Bytes.putInt(shallowBuffer, pos, offset);                   // offset
    pos = Bytes.putInt(shallowBuffer, pos, KeyValueUtil.length(kv1)); // length
    offset += KeyValueUtil.length(kv1);

    KeyValueUtil.appendToByteArray(kv2, deepBuffer, offset);
    pos = Bytes.putInt(shallowBuffer, pos, 0);                    // chunk index
    pos = Bytes.putInt(shallowBuffer, pos, offset);               // offset
    pos = Bytes.putInt(shallowBuffer, pos, KeyValueUtil.length(kv2)); // length
    offset += KeyValueUtil.length(kv2);

    KeyValueUtil.appendToByteArray(kv3, deepBuffer, offset);
    pos = Bytes.putInt(shallowBuffer, pos, 0);                    // chunk index
    pos = Bytes.putInt(shallowBuffer, pos, offset);               // offset
    pos = Bytes.putInt(shallowBuffer, pos, KeyValueUtil.length(kv3)); // length
    /*---------------- shallow cells to bytes end   ----------------*/
  }

  public void testBasics() throws Exception {

    assertEquals(3, this.cbSet.size());   // check size

    assertTrue(this.cbSet.contains(cells[0]));  // check first
    Cell first = this.cbSet.first();
    assertTrue(cells[0].equals(first));

    assertTrue(this.cbSet.contains(cells[NUM_OF_CELLS - 1]));  // check last
    Cell last = this.cbSet.last();
    assertTrue(cells[NUM_OF_CELLS - 1].equals(last));

    SortedSet<Cell> tail = this.cbSet.tailSet(cells[1]);  // check tail abd head sizes
    assertEquals(2, tail.size());
    SortedSet<Cell> head = this.cbSet.headSet(cells[1]);
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

  public void testIterators() throws Exception {

    // Assert that we have NUM_OF_CELLS values and that they are in order
    int count = 0;
    for (Cell kv: this.cbSet) {
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
    for (Iterator<Cell> i = this.cbSet.descendingIterator(); i.hasNext();) {
      Cell kv = i.next();
      assertEquals(cells[NUM_OF_CELLS - (count + 1)], kv);
      count++;
    }
    assertEquals(NUM_OF_CELLS, count);
  }



  /*---------------------------------- Shallow Cell ----------------------------------*/
  static class ShallowCell implements Cell {
    @Override public byte[] getRowArray() {
      return new byte[0];
    }

    @Override public int getRowOffset() {
      return 0;
    }

    @Override public short getRowLength() {
      return 0;
    }

    @Override public byte[] getFamilyArray() {
      return new byte[0];
    }

    @Override public int getFamilyOffset() {
      return 0;
    }

    @Override public byte getFamilyLength() {
      return 0;
    }

    @Override public byte[] getQualifierArray() {
      return new byte[0];
    }

    @Override public int getQualifierOffset() {
      return 0;
    }

    @Override public int getQualifierLength() {
      return 0;
    }

    @Override public long getTimestamp() {
      return 0;
    }

    @Override public byte getTypeByte() {
      return 0;
    }

    @Override public long getSequenceId() {
      return 0;
    }

    @Override public byte[] getValueArray() {
      return new byte[0];
    }

    @Override public int getValueOffset() {
      return 0;
    }

    @Override public int getValueLength() {
      return 0;
    }

    @Override public byte[] getTagsArray() {
      return new byte[0];
    }

    @Override public int getTagsOffset() {
      return 0;
    }

    @Override public int getTagsLength() {
      return 0;
    }
  }
}
