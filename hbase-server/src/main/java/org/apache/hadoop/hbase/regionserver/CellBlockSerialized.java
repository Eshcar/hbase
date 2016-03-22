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

import java.util.Comparator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;



/**
 * CellBlockSerialized is a byte array holding all that is needed to access a Cell, which
 * is actually saved on another deeper byte array.
 * Per Cell we have a reference to this deeper byte array B, offset in bytes in B (integer),
 * and length in bytes in B (integer). In order to save reference to byte array we use the Chunk's
 * indexes given by MSLAB (also integer).
 *
 * The B memory layout:
 *
 * <-----------------     first Cell     ---------------------> <-------------- second Cell
 * ------------------------------------------------------------------------------------- ...
 * | integer = x bytes | integer = x bytes | integer = x bytes | integer = x bytes  |
 * |  reference to B   | offset in B where | length of Cell's  | reference to may be|    ...
 * | holding Cell data | Cell's data starts|    data in B      | another byte array |
 * ------------------------------------------------------------------------------------- ...
 */
public class CellBlockSerialized extends CellBlock {

  private HeapMemStoreLAB.Chunk[] chunks;
  private final HeapMemStoreLAB memStoreLAB;
  private int numOfCellsInsideChunk;
  private static final int BYTES_IN_CELL = 3*(Integer.SIZE / Byte.SIZE); // each Cell requires 3 integers

  public CellBlockSerialized(Comparator<? super Cell> comparator, HeapMemStoreLAB memStoreLAB,
      HeapMemStoreLAB.Chunk[] chunks, int min, int max, int chunkSize, boolean d) {
    super(comparator,min,max, d);
    this.chunks = chunks;
    this.memStoreLAB = memStoreLAB;
    this.numOfCellsInsideChunk = chunkSize / BYTES_IN_CELL;
  }

  /* To be used by base class only to create a sub-CellBlock */
  @Override
  protected CellBlock createCellBlocks(Comparator<? super Cell> comparator,
      int min, int max, boolean d) {
    return new CellBlockSerialized(comparator, this.memStoreLAB, this.chunks, min, max,
        this.numOfCellsInsideChunk* BYTES_IN_CELL, d);
  }

  @Override
  protected Cell getCellFromIndex(int i) {
    // find correct chunk
    int chunkIndex = (i / numOfCellsInsideChunk);
    byte[] block = chunks[chunkIndex].getData();
    i = i - chunkIndex*numOfCellsInsideChunk;

    // find inside chunk
    int offsetInBytes = i* BYTES_IN_CELL;
    int chunkId = Bytes.toInt(block,offsetInBytes);
    int offsetOfCell = Bytes.toInt(block,offsetInBytes+(Integer.SIZE / Byte.SIZE));
    int lengthOfCell = Bytes.toInt(block,offsetInBytes+2*(Integer.SIZE / Byte.SIZE));
    byte[] chunk = memStoreLAB.translateIdToChunk(chunkId).getData();

    Cell result = new KeyValue(chunk, offsetOfCell, lengthOfCell);
    return result;
  }
}
