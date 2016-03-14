/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Cellersion 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY CellIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;
import junit.framework.Assert;

/**
 * CellBlocksOffHeap is a byte array holding all that is needed to access a Cell, which
 * is actually saved on another deeper byte array
 */
public class CellBlocksOffHeap extends CellBlocks {

  private byte block[];
  private final HeapMemStoreLAB memStoreLAB;

  public CellBlocksOffHeap(Comparator<? super Cell> comparator, HeapMemStoreLAB memStoreLAB,
      byte b[], int min, int max, boolean d) {
    super(comparator,min,max,d);
    this.block = b;
    this.memStoreLAB = memStoreLAB;
  }

  @Override
  protected CellBlocks createCellBlocks(Comparator<? super Cell> comparator, int min, int max,
      boolean d) {
    return new CellBlocksOffHeap(comparator, this.memStoreLAB, this.block, min, max, d);
  }

  @Override
  protected Cell getCellFromIndex(int i) {
//    org.junit.Assert.assertTrue("\nGetting Cell from index:" + i + "\n",false);

    int offsetInBytes = 3*i*(Integer.SIZE / Byte.SIZE);

//    org.junit.Assert.assertTrue("\n\nGetting Cell from index: " + i
//        + ", offset to the start: " + offsetInBytes + ", the start of bytes array: " + block
//        + "\n\n",false);

    int chunkId = Bytes.toInt(block,offsetInBytes);
    int offsetOfCell = Bytes.toInt(block,offsetInBytes+(Integer.SIZE / Byte.SIZE));
    int lengthOfCell = Bytes.toInt(block,offsetInBytes+2*(Integer.SIZE / Byte.SIZE));
    byte[] chunk = memStoreLAB.translateIdToChunk(chunkId).getData();

//    org.junit.Assert.assertTrue("\n\n<<<<<< Getting Cell from index: " + i
//        + "(got deep chunk), offset to the start: " + offsetInBytes + ", the start of bytes array: "
//        + block + ", the (deep) chunk id: " + chunkId + ", offset of cell in deep buffer: "
//        + offsetOfCell + "\n\n",false);

    Cell result = new KeyValue(chunk, offsetOfCell, lengthOfCell);
    return result;
  }
}
