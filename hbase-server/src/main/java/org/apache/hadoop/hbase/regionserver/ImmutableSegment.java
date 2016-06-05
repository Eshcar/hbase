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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * ImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and is not needed for a {@link MutableSegment}. Specifically, the method
 * {@link ImmutableSegment#getKeyValueScanner()} builds a special scanner for the
 * {@link MemStoreSnapshot} object.
 */
@InterfaceAudience.Private
public class ImmutableSegment extends Segment {
  /**
   * This is an immutable segment so use the read-only TimeRange rather than the heavy-weight
   * TimeRangeTracker with all its synchronization when doing time range stuff.
   */
  private final TimeRange timeRange;

  /**
   * Types of ImmutableSegment
   */
  public enum Type {
    SKIPLIST_MAP_BASED,
    ARRAY_MAP_BASED,
    CHUNK_MAP_BASED
  }

  private Type type = Type.SKIPLIST_MAP_BASED;

  // whether it is based on CellFlatMap or ConcurrentSkipListMap
  private boolean isFlat(){
    return (type == Type.ARRAY_MAP_BASED) || (type == Type.CHUNK_MAP_BASED);
  }

  /////////////////////  CONSTRUCTORS  /////////////////////
  /**------------------------------------------------------------------------
   * Copy C-tor to be used when new ImmutableSegment is being built from a Mutable one.
   * This C-tor should be used when active MutableSegment is pushed into the compaction
   * pipeline and becomes an ImmutableSegment.
   */
  protected ImmutableSegment(Segment segment) {
    super(segment);
    type = Type.SKIPLIST_MAP_BASED;
    TimeRangeTracker trt = getTimeRangeTracker();
    this.timeRange =  trt == null? null: trt.toTimeRange();
  }

  /**------------------------------------------------------------------------
   * C-tor to be used when new ImmutableSegment is a result of compaction of a list
   * of older ImmutableSegments.
   * The given iterator returns the Cells that "survived" the compaction.
   * According to the boolean parameter "array" the new ImmutableSegment is built based on
   * CellArrayMap or CellChunkMap.
   */
  protected ImmutableSegment(
      final Configuration conf, CellComparator comparator, MemStoreCompactorIterator iterator,
      MemStoreLAB memStoreLAB, int numOfCells, Type type) {

    super(null, comparator, memStoreLAB,
        CompactingMemStore.DEEP_OVERHEAD_PER_PIPELINE_FLAT_ARRAY_ITEM,
        (type == Type.ARRAY_MAP_BASED) ?
            ClassSize.CELL_ARRAY_MAP_ENTRY : ClassSize.CELL_CHUNK_MAP_ENTRY);

    CellSet cs = null; // build the CellSet Cell array or Byte array based
    if (type == Type.ARRAY_MAP_BASED) {
      cs = createCellArrayMapSet(numOfCells, iterator);
    } else {
//      org.junit.Assert.assertTrue("\n<<<< Creating CellChunkMap set for " + numOfCells
//          + " cells. \n", false);
      cs = createCellChunkMapSet(numOfCells, iterator, conf);
    }
    this.setCellSet(null, cs);  // update the CellSet of the new Segment
    this.type = type;
    TimeRangeTracker trt = getTimeRangeTracker();
    this.timeRange =  trt == null? null: trt.toTimeRange();
  }

  /**------------------------------------------------------------------------
   * C-tor to be used when new SKIP-LIST BASED ImmutableSegment is a result of compaction of a
   * list of older ImmutableSegments.
   * The given iterator returns the Cells that "survived" the compaction.
   */
  protected ImmutableSegment(
      CellComparator comparator, MemStoreCompactorIterator iterator, MemStoreLAB memStoreLAB) {

    super(new CellSet(comparator), comparator, memStoreLAB,
        CompactingMemStore.DEEP_OVERHEAD_PER_PIPELINE_ITEM, ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);

    while (iterator.hasNext()) {
      Cell c = iterator.next();
      // The scanner is doing all the elimination logic
      // now we just copy it to the new segment
      KeyValue kv = KeyValueUtil.ensureKeyValue(c);
      Cell newKV = maybeCloneWithAllocator(kv);
      internalAdd(newKV);
    }
    type = Type.SKIPLIST_MAP_BASED;
    TimeRangeTracker trt = getTimeRangeTracker();
    this.timeRange =  trt == null? null: trt.toTimeRange();
  }

  /////////////////////  PUBLIC METHODS  /////////////////////
  /**
   * Builds a special scanner for the MemStoreSnapshot object that is different than the
   * general segment scanner.
   * @return a special scanner for the MemStoreSnapshot object
   */
  public KeyValueScanner getKeyValueScanner() {
    return new CollectionBackedScanner(getCellSet(), getComparator());
  }

  @Override
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return this.timeRange.includesTimeRange(scan.getTimeRange()) &&
        this.timeRange.getMax() >= oldestUnexpiredTS;
  }

  @Override
  public long getMinTimestamp() {
    return this.timeRange.getMin();
  }

  /**------------------------------------------------------------------------
   * Change the CellSet of this ImmutableSegment from one based on ConcurrentSkipListMap to one
   * based on CellArrayMap.
   * If this ImmutableSegment is not based on ConcurrentSkipListMap , this is NOP
   * For now the change from ConcurrentSkipListMap to CellChunkMap is not supported, because
   * this requires the Cell to know on which Chunk it is placed.
   *
   * Synchronization of the CellSet replacement:
   * The reference to the CellSet is AtomicReference and is updated only when ImmutableSegment
   * is constructed (single thread) or flattened. The flattening happens as part of a single
   * thread of compaction, but to be on the safe side the initial CellSet is locally saved
   * before the flattening and then replaced using CAS instruction.
   */
  public boolean flatten() {
    if (isFlat()) return false;
    CellSet oldCellSet = getCellSet();
    int numOfCells = getCellsCount();

    // each Cell is now represented in either in CellArrayMap or in CellChunkMap
    constantCellMetaDataSize = ClassSize.CELL_ARRAY_MAP_ENTRY;

    // arrange the meta-data size, decrease all meta-data sizes related to SkipList
    incSize(
        -(ClassSize.CONCURRENT_SKIPLISTMAP + numOfCells * ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY));
    // add size of CellArrayMap and meta-data overhead per Cell
    incSize(ClassSize.CELL_CHUNK_MAP + numOfCells * constantCellMetaDataSize);

    CellSet  newCellSet = recreateCellArrayMapSet(numOfCells); // build the CellSet CellArrayMap based
    setCellSet(oldCellSet,newCellSet);
    return true;
  }

  /////////////////////  PRIVATE METHODS  /////////////////////
  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellArrayMap from compacting iterator
  private CellSet createCellArrayMapSet(int numOfCells, MemStoreCompactorIterator iterator) {

    Cell[] cells = new Cell[numOfCells];   // build the Cell Array
    int i = 0;
    while (iterator.hasNext()) {
      Cell c = iterator.next();
      // The scanner behind the iterator is doing all the elimination logic
      // now we just copy it to the new segment (also MSLAB copy)
      KeyValue kv = KeyValueUtil.ensureKeyValue(c);
      cells[i++] = maybeCloneWithAllocator(kv);
      // last parameter false, because in compaction count both Heap (Data) and MetaData size
      updateMetaInfo(c,true);
    }
    // build the immutable CellSet
    CellArrayMap cam = new CellArrayMap(getComparator(),cells,0,i,false);
    return new CellSet(cam);
  }

  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellArrayMap from current ConcurrentSkipListMap based CellSet
  private CellSet recreateCellArrayMapSet(int numOfCells) {

    Cell[] cells = new Cell[numOfCells];   // build the Cell Array
    Cell curCell;
    int idx = 0;
    // create this segment scanner with maximal possible read point, to go over all Cells
    SegmentScanner segmentScanner = this.getSegmentScanner(Long.MAX_VALUE);

    try {
      while ((curCell = segmentScanner.next()) != null) {
        cells[idx++] = curCell;
      }
    } catch (IOException ie) {
      throw new IllegalStateException(ie);
    }
    // build the immutable CellSet
    CellArrayMap cam = new CellArrayMap(getComparator(),cells,0,idx,false);
    return new CellSet(cam);
  }

  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellChunkMap from compacting iterator
  // we do not consider cells bigger than chunks
  private CellSet createCellChunkMapSet(
      int numOfCells, MemStoreCompactorIterator iterator, final Configuration conf) {

    // calculate how many chunks we will need for metadata
    int chunkSize = conf.getInt(HeapMemStoreLAB.CHUNK_SIZE_KEY, HeapMemStoreLAB.CHUNK_SIZE_DEFAULT);
    int numOfCellsInChunk = chunkSize / CellChunkMap.BYTES_IN_CELL;
    int numberOfChunks = numOfCells/numOfCellsInChunk;

    // all Chunks (for metadata and for data) are allocated from the current segment's MSLAB
    // TODO: when Chunk is going to be out of HeapMemStoreLAB we can use MemStoreLAB here
    // and not HeapMemStoreLAB
    HeapMemStoreLAB ms = (HeapMemStoreLAB)getMemStoreLAB();
    HeapMemStoreLAB.Chunk[] chunks = new HeapMemStoreLAB.Chunk[numberOfChunks]; // metadata chunks
    int currentChunkIdx = 0;
    chunks[currentChunkIdx] = ms.allocateChunk();
    int offsetInCurentChunk = 0;

    org.junit.Assert.assertTrue("\n<<<< Creating CellChunkMap set for " + numOfCells
        + " cells. The calculated chunk size is " + chunkSize + " bytes. " + "We need "
        + numberOfChunks + " chunks. " + "\n", false);

    while (iterator.hasNext()) {
      Cell c = iterator.next();

      if (offsetInCurentChunk + CellChunkMap.BYTES_IN_CELL > chunkSize) {
        // continue to the next metadata chunk
        currentChunkIdx++;
        chunks[currentChunkIdx] = ms.allocateChunk();
        offsetInCurentChunk = 0;
      }

      // The scanner behind the iterator is doing all the elimination logic
      // now we just copy it to the new segment (also MSLAB copy)
      KeyValue kv = KeyValueUtil.ensureKeyValue(c);
      offsetInCurentChunk =
          cloneAndReference(kv, chunks[currentChunkIdx].getData(), offsetInCurentChunk);
      // last parameter false, because in compaction count both Heap (Data) and MetaData size
      updateMetaInfo(c,true);
    }

    CellChunkMap ccm = new CellChunkMap(getComparator(),(HeapMemStoreLAB)getMemStoreLAB(),
        chunks,0,numOfCells,chunkSize,false);
    return new CellSet(ccm);
  }

  /*------------------------------------------------------------------------*/
  // for a given cell, allocate space and write the cell on the data chunk,
  // then write the cell-reference on the metadata chunk
  private int cloneAndReference(Cell cell, byte[] referencesByteArray, int offsetForReference) {
    HeapMemStoreLAB ms = (HeapMemStoreLAB)getMemStoreLAB();
    int len = KeyValueUtil.length(cell);
    int offset = offsetForReference;
    // we assume Cell length is not bigger than Chunk

    // allocate
    Pair<SimpleMutableByteRange, Integer> tuple = ms.allocateBytesWithID(len);
    ByteRange alloc = tuple.getFirst();
    int chunkId = tuple.getSecond();
    KeyValueUtil.appendToByteArray(cell, alloc.getBytes(), alloc.getOffset());

    // write the reference
    offset = Bytes.putInt(referencesByteArray, offset, chunkId);           // write chunk id
    offset = Bytes.putInt(referencesByteArray, offset, alloc.getOffset()); // offset
    offset = Bytes.putInt(referencesByteArray, offset, len);               // length
    return offset;
  }
}
