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

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ByteBufferUtils;

import java.util.Comparator;


/**
 * CellChunkMap is an array of serialized representations of Cell
 * (pointing to Chunks with full Cell data) and can be allocated both off-heap and on-heap.
 *
 * CellChunkMap is a byte array (chunk) holding all that is needed to access a Cell, which
 * is actually saved on another deeper byte array (chunk).
 * Per Cell we have a reference to this deeper byte array B (chunk ID, integer),
 * offset in bytes in B (integer), length in bytes in B (integer) and seqID of the cell (long).
 * In order to save reference to byte array we use the Chunk's ID given by ChunkCreator.
 *
 * The CellChunkMap memory layout relevant to a deeper byte array B, holding the actual cell data:
 *
 * <-----------------     first Cell     --------------------------------------> <- second Cell ...
 * --------------------------------------------------------------------------------------- ...
 * | integer: 4 bytes  | integer: 4 bytes  | integer: 4 bytes  | long: 8 bytes  |
 * | chunkID of chunk B| offset in B where | length of Cell's  | sequence ID of |          ...
 * | holding Cell data | Cell's data starts|    data in B      | the Cell       |
 * --------------------------------------------------------------------------------------- ...
 */
@InterfaceAudience.Private
public class CellChunkMap extends CellFlatMap {

  private final Chunk[] chunks;             // the array of chunks, on which the index is based
  private final int numOfCellsInsideChunk;  // constant number of cell-representations in a chunk
  private final ChunkCreator chunkCreator;  // ChunkCreator for chunkID translation

  // TODO: move this constant to Cell or ExtendedCell, does not belong here...
  // each Cell requires three integers for chunkID (reference to the ByteBuffer), offset
  // and length, and one long for seqID
  public static final int SIZEOF_CELL_REF = 3*Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG ;

  /* C-tor for creating CellChunkMap from existing Chunk array, which must be ordered
  *   (decreasingly or increasingly according to parameter "descending")
  * min - the index of the first cell (usually 0)
  * max - number of Cells or the index of the cell after the maximal cel */
  public CellChunkMap(
      Comparator<? super Cell> comparator,
      Chunk[] chunks, int min, int max, boolean descending) {
    super(comparator, min, max, descending);
    this.chunkCreator = ChunkCreator.getInstance();
    this.chunks = chunks;
    //TODO: change to int when the chunk ID size bug is fixed
    this.numOfCellsInsideChunk = // each chunk starts with its own ID following the cells data
        (chunkCreator.getChunkSize() - Bytes.SIZEOF_LONG) / SIZEOF_CELL_REF;
  }

  /* To be used by base (CellFlatMap) class only to create a sub-CellFlatMap
  * Should be used only to create only CellChunkMap from CellChunkMap */
  @Override
  protected CellFlatMap createSubCellFlatMap(int min, int max, boolean descending) {
    return new CellChunkMap(this.comparator(), this.chunks, min, max, descending);
  }


  @Override
  protected Cell getCell(int i) {
    // get the index of the relevant chunk inside chunk array
    int chunkIndex = (i / numOfCellsInsideChunk);
    ByteBuffer block = chunks[chunkIndex].getData();// get the ByteBuffer of the relevant chunk
    i = i - chunkIndex * numOfCellsInsideChunk;     // get the index of the cell-representation

    // find inside the offset inside the chunk holding the index, skip bytes for chunk id
    //TODO: change to int when the chunk ID size bug is fixed
    int offsetInBytes = Bytes.SIZEOF_LONG + i*SIZEOF_CELL_REF;

    // find the chunk holding the data of the cell, the chunkID is stored first
    int chunkId = ByteBufferUtils.toInt(block, offsetInBytes);
    Chunk chunk = chunkCreator.getChunk(chunkId);
    if (chunk == null) {
      // this should not happen, putting an assertion here at least for the testing period
      assert false;
    }

    // find the offset of the data of the cell, skip integer for chunkID, offset is stored second
    int offsetOfCell = ByteBufferUtils.toInt(block, offsetInBytes + Bytes.SIZEOF_INT);
    // find the length of the data of the cell, skip two integers for chunkID and offset,
    // length is stored third
    int lengthOfCell = ByteBufferUtils.toInt(block, offsetInBytes + 2*Bytes.SIZEOF_INT);
    // find the seqID of the cell, skip three integers for chunkID, offset, and length
    // the seqID is plain written as part of the cell representation
    long cellSeqID = ByteBufferUtils.toLong(block, offsetInBytes + 3*Bytes.SIZEOF_INT);

    ByteBuffer buf = chunk.getData();   // get the ByteBuffer where the cell data is stored
    if (chunk == null) {
      // this should not happen, putting an assertion here at least for the testing period
      assert false;
    }

    return new ByteBufferChunkCell(buf, offsetOfCell, lengthOfCell, cellSeqID);
  }
}
