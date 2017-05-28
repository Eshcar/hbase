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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and is not needed for a {@link MutableSegment}.
 */
@InterfaceAudience.Private
public class ImmutableSegment extends Segment {

  private static final long DEEP_OVERHEAD = Segment.DEEP_OVERHEAD
      + (2 * ClassSize.REFERENCE) // Refs to timeRange and type
      + ClassSize.TIMERANGE;
  public static final long DEEP_OVERHEAD_CSLM = DEEP_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP;
  public static final long DEEP_OVERHEAD_CAM = DEEP_OVERHEAD + ClassSize.CELL_ARRAY_MAP;

  /**
   * Types of ImmutableSegment
   */
  public enum Type {
    SKIPLIST_MAP_BASED,
    ARRAY_MAP_BASED,
    CHUNK_MAP_BASED
  }

  /**
   * This is an immutable segment so use the read-only TimeRange rather than the heavy-weight
   * TimeRangeTracker with all its synchronization when doing time range stuff.
   */
  private final TimeRange timeRange;

  private Type type = Type.SKIPLIST_MAP_BASED;

  // whether it is based on CellFlatMap or ConcurrentSkipListMap
  private boolean isFlat(){
    return (type != Type.SKIPLIST_MAP_BASED);
  }

  /////////////////////  CONSTRUCTORS  /////////////////////
  /**------------------------------------------------------------------------
   * Empty C-tor to be used only for CompositeImmutableSegment
   */
  protected ImmutableSegment(CellComparator comparator) {
    super(comparator);
    this.timeRange = null;
  }

  /**------------------------------------------------------------------------
   * Copy C-tor to be used when new ImmutableSegment is being built from a Mutable one.
   * This C-tor should be used when active MutableSegment is pushed into the compaction
   * pipeline and becomes an ImmutableSegment.
   */
  protected ImmutableSegment(Segment segment) {
    super(segment);
    this.type = Type.SKIPLIST_MAP_BASED;
    this.timeRange = this.timeRangeTracker == null ? null : this.timeRangeTracker.toTimeRange();
  }

  /**------------------------------------------------------------------------
   * C-tor to be used when new CELL_ARRAY BASED ImmutableSegment is a result of compaction of a
   * list of older ImmutableSegments.
   * The given iterator returns the Cells that "survived" the compaction.
   * The input parameter "type" exists for future use when more types of flat ImmutableSegments
   * are going to be introduced.
   */
  protected ImmutableSegment(CellComparator comparator, MemStoreSegmentsIterator iterator,
      MemStoreLAB memStoreLAB, int numOfCells, Type type, boolean merge, boolean toCellChunkMap) {

    super(null, // initiailize the CellSet with NULL
        comparator, memStoreLAB);
    this.type = type;
    // build the new CellSet based on CellArrayMap
    CellSet cs = toCellChunkMap ?
        createCellChunkMapSet(numOfCells, iterator, merge) :
        createCellArrayMapSet(numOfCells, iterator, merge);

    this.setCellSet(null, cs);            // update the CellSet of the new Segment
    this.timeRange = this.timeRangeTracker == null ? null : this.timeRangeTracker.toTimeRange();
  }

  /**------------------------------------------------------------------------
   * C-tor to be used when new SKIP-LIST BASED ImmutableSegment is a result of compaction of a
   * list of older ImmutableSegments.
   * The given iterator returns the Cells that "survived" the compaction.
   *
   * currently not in usage
   */
  protected ImmutableSegment(CellComparator comparator, MemStoreSegmentsIterator iterator,
      MemStoreLAB memStoreLAB) {
    super(new CellSet(comparator), // initiailize the CellSet with empty CellSet
        comparator, memStoreLAB);
    type = Type.SKIPLIST_MAP_BASED;
    while (iterator.hasNext()) {
      Cell c = iterator.next();
      // The scanner is doing all the elimination logic
      // now we just copy it to the new segment
      Cell newKV = maybeCloneWithAllocator(c);
      boolean usedMSLAB = (newKV != c);
      internalAdd(newKV, usedMSLAB, null);
    }
    this.timeRange = this.timeRangeTracker == null ? null : this.timeRangeTracker.toTimeRange();
  }

  /////////////////////  PUBLIC METHODS  /////////////////////

  @Override
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return this.timeRange.includesTimeRange(scan.getTimeRange()) &&
        this.timeRange.getMax() >= oldestUnexpiredTS;
  }

  @Override
  public long getMinTimestamp() {
    return this.timeRange.getMin();
  }

  public int getNumOfSegments() {
    return 1;
  }

  public List<Segment> getAllSegments() {
    List<Segment> res = new ArrayList<>(Arrays.asList(this));
    return res;
  }

  /**------------------------------------------------------------------------
   * Change the CellSet of this ImmutableSegment from one based on ConcurrentSkipListMap to one
   * based on CellArrayMap.
   * If this ImmutableSegment is not based on ConcurrentSkipListMap , this is NOOP
   *
   * Synchronization of the CellSet replacement:
   * The reference to the CellSet is AtomicReference and is updated only when ImmutableSegment
   * is constructed (single thread) or flattened. The flattening happens as part of a single
   * thread of compaction, but to be on the safe side the initial CellSet is locally saved
   * before the flattening and then replaced using CAS instruction.
   */
  public boolean flatten(MemstoreSize memstoreSize, boolean toCellChunkMap) {
    if (isFlat()) return false;
    CellSet oldCellSet = getCellSet();
    int numOfCells = getCellsCount();

    // build the new (CellSet CellArrayMap based)
    CellSet  newCellSet = toCellChunkMap ?
        recreateCellChunkMapSet(numOfCells) : recreateCellArrayMapSet(numOfCells) ;
    type = toCellChunkMap ? Type.CHUNK_MAP_BASED : Type.ARRAY_MAP_BASED;
    setCellSet(oldCellSet,newCellSet);

    // arrange the meta-data size, decrease all meta-data sizes related to SkipList;
    // if flattening is to CellChunkMap decrease also Cell object sizes
    // (recreateCellArrayMapSet doesn't take the care for the sizes)
    long newSegmentSizeDelta = -(numOfCells * (ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY+
        (toCellChunkMap ? KeyValue.FIXED_OVERHEAD : 0)));
    // add size of CellArrayMap entry or CellChunkMap entry
    newSegmentSizeDelta = newSegmentSizeDelta + numOfCells *
        (toCellChunkMap ? CellChunkMap.SIZEOF_CELL_REP : ClassSize.CELL_ARRAY_MAP_ENTRY);
    incSize(0, newSegmentSizeDelta);
    if (memstoreSize != null) {
      memstoreSize.incMemstoreSize(0, newSegmentSizeDelta);
    }

    return true;
  }

  /////////////////////  PRIVATE METHODS  /////////////////////

  @Override
  protected long heapSizeChange(Cell cell, boolean succ) {
    if (succ) {
      switch (this.type) {
      case SKIPLIST_MAP_BASED:
        return super.heapSizeChange(cell, succ);
      case ARRAY_MAP_BASED:
        return ClassSize.align(ClassSize.CELL_ARRAY_MAP_ENTRY + CellUtil.estimatedHeapSizeOf(cell));
      case CHUNK_MAP_BASED:
        return ClassSize.align( // no Cell object is created so subtracting KeyValue.FIXED_OVERHEAD
            CellChunkMap.SIZEOF_CELL_REP + CellUtil.estimatedHeapSizeOf(cell)
                - KeyValue.FIXED_OVERHEAD);
      }
    }
    return 0;
  }

  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellArrayMap from compacting iterator
  private CellSet createCellArrayMapSet(int numOfCells, MemStoreSegmentsIterator iterator,
      boolean merge) {

    Cell[] cells = new Cell[numOfCells];   // build the Cell Array
    int i = 0;
    while (iterator.hasNext()) {
      Cell c = iterator.next();
      // The scanner behind the iterator is doing all the elimination logic
      if (merge) {
        // if this is merge we just move the Cell object without copying MSLAB
        // the sizes still need to be updated in the new segment
        cells[i] = c;
      } else {
        // now we just copy it to the new segment (also MSLAB copy)
        cells[i] = maybeCloneWithAllocator(c);
      }
      boolean useMSLAB = (getMemStoreLAB()!=null);
      // second parameter true, because in compaction/merge the addition of the cell to new segment
      // is always successful
      updateMetaInfo(c, true, useMSLAB, null); // updates the size per cell
      i++;
    }
    // build the immutable CellSet
    CellArrayMap cam = new CellArrayMap(getComparator(), cells, 0, i, false);
    return new CellSet(cam);
  }

  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellArrayMap from current ConcurrentSkipListMap based CellSet
  // (without compacting iterator)
  private CellSet recreateCellArrayMapSet(int numOfCells) {

    Cell[] cells = new Cell[numOfCells];   // build the Cell Array
    Cell curCell;
    int idx = 0;
    // create this segment scanner with maximal possible read point, to go over all Cells
    KeyValueScanner segmentScanner = this.getScanner(Long.MAX_VALUE);

    try {
      while ((curCell = segmentScanner.next()) != null) {
        cells[idx++] = curCell;
      }
    } catch (IOException ie) {
      throw new IllegalStateException(ie);
    } finally {
      segmentScanner.close();
    }

    // build the immutable CellSet
    CellArrayMap cam = new CellArrayMap(getComparator(), cells, 0, idx, false);
    return new CellSet(cam);
  }

  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellArrayMap from compacting iterator
  private CellSet createCellChunkMapSet(int numOfCells, MemStoreSegmentsIterator iterator,
      boolean merge) {

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
      chunks[i] = ChunkCreator.getInstance().getChunk();
    }

    while (iterator.hasNext()) {        // the iterator hides the elimination logic for compaction
      Cell c = iterator.next();
      numOfCellsAfterCompaction++;
      if (!(c instanceof ExtendedCell)) // we shouldn't get here anything but ExtendedCell
        assert false;

      if (offsetInCurentChunk + CellChunkMap.SIZEOF_CELL_REP > chunkSize) {
        currentChunkIdx++;              // continue to the next index chunk
        offsetInCurentChunk = ChunkCreator.SIZEOF_CHUNK_HEADER;
      }

      if (!merge) {
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
  // We do not consider cells bigger than chunks!
  private CellSet recreateCellChunkMapSet(int numOfCells) {
    // create this segment scanner with maximal possible read point, to go over all Cells
    KeyValueScanner segmentScanner = this.getScanner(Long.MAX_VALUE);
    Cell curCell;

    // calculate how many chunks we will need for metadata
    int chunkSize = ChunkCreator.getInstance().getChunkSize();
    int numOfCellsInChunk = CellChunkMap.NUM_OF_CELL_REPS_IN_CHUNK;
    int numberOfChunks = numOfCells/numOfCellsInChunk + 1;

    // all index Chunks are allocated from ChunkCreator
    Chunk[] chunks = new Chunk[numberOfChunks];
    for (int i=0; i<numberOfChunks; i++) {
      chunks[i] = ChunkCreator.getInstance().getChunk();
    }

    int currentChunkIdx = 0;
    int offsetInCurentChunk = ChunkCreator.SIZEOF_CHUNK_HEADER;

    try {
      while ((curCell = segmentScanner.next()) != null) {
        if (!(curCell instanceof ExtendedCell)) // we shouldn't get here anything but ExtendedCell
          assert false;
        if (offsetInCurentChunk + CellChunkMap.SIZEOF_CELL_REP > chunkSize) {
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

    offset = ByteBufferUtils.putInt(idxBuffer, offset, cell.getChunkId());    // write data chunk id
    offset = ByteBufferUtils.putInt(idxBuffer, offset, cell.getOffset());          // offset
    offset = ByteBufferUtils.putInt(idxBuffer, offset, KeyValueUtil.length(cell)); // length
    offset = ByteBufferUtils.putLong(idxBuffer, offset, cell.getSequenceId());     // seqId

    return offset;
  }


}
