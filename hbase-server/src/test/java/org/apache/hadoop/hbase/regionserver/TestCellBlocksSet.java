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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.SortedSet;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCellBlocksSet extends TestCase {

  private static final int NUM_OF_CELLS = 3;

  private Cell cells[];
  private CellBlocksMap<Cell,Cell> cbMap;
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
    cbMap = new CellBlocksMap<Cell,Cell>(CellComparator.COMPARATOR,cells,0,NUM_OF_CELLS,false);
    cbSet = new CellSet(cbMap);
  }

  /*public void testAdd() throws Exception {
    byte [] bytes = Bytes.toBytes(getName());
    KeyValue kv = new KeyValue(bytes, bytes, bytes, bytes);
    this.cbSet.add(kv);
    assertTrue(this.cbSet.contains(kv));
    assertEquals(1, this.cbSet.size());
    Cell first = this.cbSet.first();
    assertTrue(kv.equals(first));
    assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(),
      first.getValueArray(), first.getValueOffset(), first.getValueLength()));
    // Now try overwritting
    byte [] overwriteValue = Bytes.toBytes("overwrite");
    KeyValue overwrite = new KeyValue(bytes, bytes, bytes, overwriteValue);
    this.cbSet.add(overwrite);
    assertEquals(1, this.cbSet.size());
    first = this.cbSet.first();
    assertTrue(Bytes.equals(overwrite.getValueArray(), overwrite.getValueOffset(),
      overwrite.getValueLength(), first.getValueArray(), first.getValueOffset(),
      first.getValueLength()));
    assertFalse(Bytes.equals(CellUtil.cloneValue(overwrite), CellUtil.cloneValue(kv)));
  }*/

  public void testIterators() throws Exception {

    // Assert that we have NUM_OF_CELLS values and that they are in order
    int count = 0;
    for (Cell kv: this.cbSet) {
      assertEquals("" + count,
          Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()));
      count++;
    }
    assertEquals(NUM_OF_CELLS, count);

    // Test descending iterator
    count = 0;
    for (Iterator<Cell> i = this.cbSet.descendingIterator(); i.hasNext();) {
      Cell kv = i.next();
      assertEquals("" + (NUM_OF_CELLS - (count + 1)),
          Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()));
      count++;
    }
    assertEquals(NUM_OF_CELLS, count);
  }

//  public void testDescendingIterator() throws Exception {
//    byte [] bytes = Bytes.toBytes(getName());
//    byte [] value1 = Bytes.toBytes("1");
//    byte [] value2 = Bytes.toBytes("2");
//    final int total = 3;
//    for (int i = 0; i < total; i++) {
//      this.cbSet.add(new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value1));
//    }
//    // Assert that we added 'total' values and that they are in order
//    int count = 0;
//    for (Iterator<Cell> i = this.cbSet.descendingIterator(); i.hasNext();) {
//      Cell kv = i.next();
//      assertEquals("" + (total - (count + 1)),
//        Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()));
//      assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(), value1,
//        0, value1.length));
//      count++;
//    }
//    assertEquals(total, count);
//    // Now overwrite with a new value.
//    for (int i = 0; i < total; i++) {
//      this.cbSet.add(new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value2));
//    }
//    // Assert that we added 'total' values and that they are in order and that
//    // we are getting back value2
//    count = 0;
//    for (Iterator<Cell> i = this.cbSet.descendingIterator(); i.hasNext();) {
//      Cell kv = i.next();
//      assertEquals("" + (total - (count + 1)),
//        Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()));
//      assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(), value2,
//        0, value2.length));
//      count++;
//    }
//    assertEquals(total, count);
//  }
//
//  public void testHeadTail() throws Exception {
//    byte [] bytes = Bytes.toBytes(getName());
//    byte [] value1 = Bytes.toBytes("1");
//    byte [] value2 = Bytes.toBytes("2");
//    final int total = 3;
//    KeyValue splitter = null;
//    for (int i = 0; i < total; i++) {
//      KeyValue kv = new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value1);
//      if (i == 1) splitter = kv;
//      this.cbSet.add(kv);
//    }
//    SortedSet<Cell> tail = this.cbSet.tailSet(splitter);
//    assertEquals(2, tail.size());
//    SortedSet<Cell> head = this.cbSet.headSet(splitter);
//    assertEquals(1, head.size());
//    // Now ensure that we get back right answer even when we do tail or head.
//    // Now overwrite with a new value.
//    for (int i = 0; i < total; i++) {
//      this.cbSet.add(new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value2));
//    }
//    tail = this.cbSet.tailSet(splitter);
//    assertTrue(Bytes.equals(tail.first().getValueArray(), tail.first().getValueOffset(),
//      tail.first().getValueLength(), value2, 0, value2.length));
//    head = this.cbSet.headSet(splitter);
//    assertTrue(Bytes.equals(head.first().getValueArray(), head.first().getValueOffset(),
//      head.first().getValueLength(), value2, 0, value2.length));
//  }
}
