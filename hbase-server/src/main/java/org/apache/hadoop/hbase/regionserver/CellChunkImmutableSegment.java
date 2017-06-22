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

import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.ClassSize;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * CSLMImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and {@link ImmutableSegment}. This immutable segment is working with CellSet with
 * CellChunkMap delegatee.
 */
@InterfaceAudience.Private
public class CellChunkImmutableSegment extends ImmutableSegment {

  public static final long DEEP_OVERHEAD_CCM =
      ImmutableSegment.DEEP_OVERHEAD + ClassSize.CELL_CHUNK_MAP;

  /////////////////////  CONSTRUCTORS  /////////////////////
  /**------------------------------------------------------------------------
   * C-tor to be used when new CellChunkImmutableSegment is built as a result of compaction/merge
   * of a list of older ImmutableSegments.
   * The given iterator returns the Cells that "survived" the compaction.
   */
  protected CellChunkImmutableSegment(CellComparator comparator, MemStoreSegmentsIterator iterator,
      MemStoreLAB memStoreLAB, int numOfCells, MemStoreCompactor.Action action) {
    super(null, comparator, memStoreLAB); // initiailize the CellSet with NULL
    incSize(0,DEEP_OVERHEAD_CCM); // initiate the heapSize with the size of the segment metadata
    // build the new CellSet based on CellArrayMap
    CellSet cs = createCellChunkMapSet(numOfCells, iterator, action);
    this.setCellSet(null, cs);            // update the CellSet of the new Segment
  }

  /**------------------------------------------------------------------------
   * C-tor to be used when new CellChunkImmutableSegment is built as a result of flattening
   * of CSLMImmutableSegment
   * The given iterator returns the Cells that "survived" the compaction.
   */
  protected CellChunkImmutableSegment(CSLMImmutableSegment segment, MemstoreSize memstoreSize) {
    super(segment); // initiailize the upper class
    incSize(0,-CSLMImmutableSegment.DEEP_OVERHEAD_CSLM+ CellChunkImmutableSegment.DEEP_OVERHEAD_CCM);
    int numOfCells = segment.getCellsCount();
    // build the new CellSet based on CellChunkMap
    CellSet cs = recreateCellFlatMapSet(numOfCells, segment.getScanner(Long.MAX_VALUE));
    this.setCellSet(segment.getCellSet(), cs);            // update the CellSet of the new Segment

    // arrange the meta-data size, decrease all meta-data sizes related to SkipList;
    // if flattening is to CellChunkMap decrease also Cell object sizes
    // (recreateCellArrayMapSet doesn't take the care for the sizes)
    long newSegmentSizeDelta =
        -(numOfCells * (ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + KeyValue.FIXED_OVERHEAD));
    // add size of CellArrayMap entry or CellChunkMap entry
    newSegmentSizeDelta =
        newSegmentSizeDelta + (numOfCells * ClassSize.CELL_CHUNK_MAP_ENTRY);
    incSize(0, newSegmentSizeDelta);
    memstoreSize.incMemstoreSize(0, newSegmentSizeDelta);
  }

  @Override
  protected long heapSizeChange(Cell cell, boolean succ) {
    if (succ) {
      return ClassSize.align( // no Cell object is created so subtracting KeyValue.FIXED_OVERHEAD
          ClassSize.CELL_CHUNK_MAP_ENTRY + CellUtil.estimatedHeapSizeOf(cell)
              - KeyValue.FIXED_OVERHEAD);
    }
    return 0;
  }

  @Override
  protected long indexEntrySize() {
    return (ClassSize.CELL_CHUNK_MAP_ENTRY - KeyValue.FIXED_OVERHEAD);
  }

  @Override protected boolean isFlat() {
    return true;
  }

  /////////////////////  PRIVATE METHODS  /////////////////////
  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellChunkMap from compacting iterator
  private CellSet createCellChunkMapSet(int numOfCells, MemStoreSegmentsIterator iterator,
      MemStoreCompactor.Action action) {

    // calculate how many chunks we will need for index
    int chunkSize = ChunkCreator.getInstance().getChunkSize();
    int numOfCellsInChunk = CellChunkMap.NUM_OF_CELL_REPS_IN_CHUNK;
    int numberOfChunks = numOfCells/numOfCellsInChunk + 1;
    int numOfCellsAfterCompaction = 0;
    int currentChunkIdx = 0;
    int offsetInCurentChunk = ChunkCreator.SIZEOF_CHUNK_HEADER;

    // all index Chunks are allocated from ChunkCreator
    Chunk[] chunks = new Chunk[numberOfChunks];
    for (int i=0; i<numberOfChunks; i++) {
      chunks[i] = this.getMemStoreLAB().getNewChunk();
    }

    while (iterator.hasNext()) {        // the iterator hides the elimination logic for compaction
      Cell c = iterator.next();
      numOfCellsAfterCompaction++;
      assert (c instanceof ByteBufferKeyValue); // shouldn't get here anything but ByteBufferKeyValue

      if (offsetInCurentChunk + ClassSize.CELL_CHUNK_MAP_ENTRY > chunkSize) {
        currentChunkIdx++;              // continue to the next index chunk
        offsetInCurentChunk = ChunkCreator.SIZEOF_CHUNK_HEADER;
      }

      if (action == MemStoreCompactor.Action.COMPACT) {
        c = maybeCloneWithAllocator(c); // for compaction copy cell to the new segment (MSLAB copy)
      }

      offsetInCurentChunk = // add the Cell reference to the index chunk
          createCellReference((ByteBufferKeyValue)c, chunks[currentChunkIdx].getData(),
              offsetInCurentChunk);

      boolean useMSLAB = (getMemStoreLAB()!=null);

      // the sizes still need to be updated in the new segment
      // second parameter true, because in compaction/merge the addition of the cell to new segment
      // is always successful
      updateMetaInfo(c, true, useMSLAB, null); // updates the size per cell
    }
    // build the immutable CellSet
    CellChunkMap ccm =
        new CellChunkMap(CellComparator.COMPARATOR,chunks,0,numOfCellsAfterCompaction,false);
    return new CellSet(ccm);
  }

  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellChunkMap from current ConcurrentSkipListMap based CellSet
  // (without compacting iterator)
  // This is a service for not-flat immutable segments
  // Assumption: cells do not exceed chunk size!
  private CellSet recreateCellFlatMapSet(int numOfCells, KeyValueScanner segmentScanner) {
    Cell curCell;
    // calculate how many chunks we will need for metadata
    int chunkSize = ChunkCreator.getInstance().getChunkSize();
    int numOfCellsInChunk = CellChunkMap.NUM_OF_CELL_REPS_IN_CHUNK;
    int numberOfChunks = numOfCells/numOfCellsInChunk + 1;
    if(numOfCells%numOfCellsInChunk==0) { // if cells can be divided evenly between chunks
      numberOfChunks--;                   // addition of the one in the calculation above is
    }                                     // not needed

    // all index Chunks are allocated from ChunkCreator
    Chunk[] chunks = new Chunk[numberOfChunks];
    for (int i=0; i<numberOfChunks; i++) {
      chunks[i] = this.getMemStoreLAB().getNewChunk();
    }

    int currentChunkIdx = 0;
    int offsetInCurentChunk = ChunkCreator.SIZEOF_CHUNK_HEADER;

    try {
      while ((curCell = segmentScanner.next()) != null) {
        assert (curCell instanceof ByteBufferKeyValue); // shouldn't get here anything but ByteBufferKeyValue
        if (offsetInCurentChunk + ClassSize.CELL_CHUNK_MAP_ENTRY > chunkSize) {
          // continue to the next metadata chunk
          currentChunkIdx++;
          offsetInCurentChunk = ChunkCreator.SIZEOF_CHUNK_HEADER;
        }
        offsetInCurentChunk =
            createCellReference((ByteBufferKeyValue) curCell, chunks[currentChunkIdx].getData(),
                offsetInCurentChunk);
      }
    } catch (IOException ie) {
      throw new IllegalStateException(ie);
    } finally {
      segmentScanner.close();
    }

    CellChunkMap ccm = new CellChunkMap(CellComparator.COMPARATOR,chunks,0,numOfCells,false);
    return new CellSet(ccm);
  }

  /*------------------------------------------------------------------------*/
  // for a given cell, write the cell representation on the index chunk
  private int createCellReference(ByteBufferKeyValue cell, ByteBuffer idxBuffer, int idxOffset) {
    int offset = idxOffset;
    int dataChunkID = cell.getChunkId();
    // ensure strong pointer to data chunk, as index is no longer directly points to it
    Chunk c = ChunkCreator.getInstance().saveChunkFromGC(dataChunkID);
    // if c is null, it means that this cell chunks was already released shouldn't happen
    assert (c!=null);
    offset = ByteBufferUtils.putInt(idxBuffer, offset, dataChunkID);    // write data chunk id
    offset = ByteBufferUtils.putInt(idxBuffer, offset, cell.getOffset());          // offset
    offset = ByteBufferUtils.putInt(idxBuffer, offset, KeyValueUtil.length(cell)); // length
    offset = ByteBufferUtils.putLong(idxBuffer, offset, cell.getSequenceId());     // seqId

    return offset;
  }


}
