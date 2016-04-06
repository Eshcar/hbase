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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;

/**
 * ImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and is not needed for a {@link MutableSegment}. Specifically, the method
 * {@link ImmutableSegment#getKeyValueScanner()} builds a special scanner for the
 * {@link MemStoreSnapshot} object.
 */
@InterfaceAudience.Private
public class ImmutableSegment extends Segment {

  private boolean isFlat; // whether it is based on CellFlatMap or ConcurrentSkipListMap

  /**
   * Builds a special scanner for the MemStoreSnapshot object that is different than the
   * general segment scanner.
   * @return a special scanner for the MemStoreSnapshot object
   */
  public KeyValueScanner getKeyValueScanner() {
    return new CollectionBackedScanner(getCellSet(), getComparator());
  }

  public boolean isFlat() {
    return isFlat;
  }

  protected ImmutableSegment(Segment segment) {
    super(segment);
    isFlat = false;
  }

  // C-tor by flattening other (old) segment
  protected ImmutableSegment(ImmutableSegment oldSegment, MemStoreCompactorIterator iterator,
      MemStoreLAB memStoreLAB, int numOfCells, long constantCellSizeOverhead) {

    super(null,oldSegment.getComparator(),memStoreLAB,
        CompactingMemStore.DEEP_OVERHEAD_PER_PIPELINE_FLAT_ARRAY_ITEM, constantCellSizeOverhead);
    CellSet cs = this.createArrayBasedCellSet(numOfCells, iterator, false); // build the CellSet
    this.setCellSet(cs);      // update the CellSet of the new Segment
    isFlat = true;
  }

  // C-tor by compaction to CellArrayMap or CellChunkMap
  protected ImmutableSegment(
      final Configuration conf, CellComparator comparator, MemStoreCompactorIterator iterator,
      MemStoreLAB memStoreLAB, int numOfCells, long constantCellSizeOverhead, boolean array) {

    super(null, comparator, memStoreLAB,
        CompactingMemStore.DEEP_OVERHEAD_PER_PIPELINE_FLAT_ARRAY_ITEM, constantCellSizeOverhead);
    CellSet cs = null; // build the CellSet Cell array or Byte array based
    if (array) {
      cs = this.createArrayBasedCellSet(numOfCells, iterator, true);
    } else {
      cs = this.createChunkBasedCellSet(numOfCells, iterator, conf);
    }
    this.setCellSet(cs);  // update the CellSet of the new Segment
    isFlat = true;
  }

  private CellSet createArrayBasedCellSet(
      int numOfCells, MemStoreCompactorIterator iterator, boolean allocateFromMSLAB) {

    Cell[] cells = new Cell[numOfCells];   // build the Cell Array
    int i = 0;
    while (iterator.hasNext()) {
      Cell c = iterator.next();
      if (allocateFromMSLAB) {
        // The scanner behind the iterator is doing all the elimination logic
        // now we just copy it to the new segment (also MSLAB copy)
        KeyValue kv = KeyValueUtil.ensureKeyValue(c);
        Cell newKV = maybeCloneWithAllocator(kv);
        cells[i++] = newKV;
        // flattening = false, compaction case, counting both Heap and MetaData size
        updateMetaInfo(c,true,!allocateFromMSLAB);
      } else {
        cells[i++] = c;
        // flattening = true, flattening case, counting only MetaData size
        updateMetaInfo(c,true,!allocateFromMSLAB);
      }

    }
    // build the immutable CellSet
    CellArrayMap cam = new CellArrayMap(getComparator(),cells,0,i,false);
    return new CellSet(cam);
  }

  private CellSet createChunkBasedCellSet(
      int numOfCells, MemStoreCompactorIterator iterator, final Configuration conf) {

    int chunkSize = conf.getInt(HeapMemStoreLAB.CHUNK_SIZE_KEY, HeapMemStoreLAB.CHUNK_SIZE_DEFAULT);
    int numOfCellsInsideChunk = chunkSize / CellChunkMap.BYTES_IN_CELL;
    int numberOfChunks = chunkSize/numOfCellsInsideChunk;
    HeapMemStoreLAB ms = (HeapMemStoreLAB)getMemStoreLAB();

    // all Chunks must be allocated from current MSLAB
    HeapMemStoreLAB.Chunk[] chunks = new HeapMemStoreLAB.Chunk[numberOfChunks];
    int currentChunkIdx = 0;
    chunks[currentChunkIdx] = ms.allocateChunk();
    int offsetInCurentChunk = 0;

    while (iterator.hasNext()) {
      Cell c = iterator.next();

      if (offsetInCurentChunk + CellChunkMap.BYTES_IN_CELL > chunkSize) {
        // we do not consider cells bigger than chunks
        // continue to the next chunk
        currentChunkIdx++;
        chunks[currentChunkIdx] = ms.allocateChunk();
        offsetInCurentChunk = 0;
      }

      // The scanner behind the iterator is doing all the elimination logic
      // now we just copy it to the new segment (also MSLAB copy)
      KeyValue kv = KeyValueUtil.ensureKeyValue(c);
      offsetInCurentChunk =
          cloneAndReference(kv, chunks[currentChunkIdx].getData(), offsetInCurentChunk);
    }

    CellChunkMap ccm = new CellChunkMap(getComparator(),(HeapMemStoreLAB)getMemStoreLAB(),
        chunks,0,numOfCells,chunkSize,false);
    return new CellSet(ccm);
  }

  /**
   * If the segment has a memory allocator the cell is being cloned to this space, and returned;
   * otherwise the given cell is returned
   * @return either the given cell or its clone
   */
  private int cloneAndReference(Cell cell, byte[] referencesByteArray, int offsetForReference) {
    HeapMemStoreLAB ms = (HeapMemStoreLAB)getMemStoreLAB();
    int len = KeyValueUtil.length(cell);
    int offset = offsetForReference;
    // we assume Cell length is not bigger than Chunk

    // allocate
    ByteRange alloc = ms.allocateBytes(len);
    int chunkId = ms.getCurrentChunkId();
    KeyValueUtil.appendToByteArray(cell, alloc.getBytes(), alloc.getOffset());

    // write the reference
    offset = Bytes.putInt(referencesByteArray, offset, chunkId);           // write chunk id
    offset = Bytes.putInt(referencesByteArray, offset, alloc.getOffset()); // offset
    offset = Bytes.putInt(referencesByteArray, offset, len);               // length
    return offset;
  }
}
