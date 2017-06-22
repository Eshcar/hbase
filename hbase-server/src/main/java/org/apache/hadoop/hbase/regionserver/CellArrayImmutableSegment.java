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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ClassSize;

import java.io.IOException;

/**
 * CSLMImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and {@link ImmutableSegment}. This immutable segment is working with CellSet with
 * CellArrayMap delegatee.
 */
@InterfaceAudience.Private
public class CellArrayImmutableSegment extends ImmutableSegment {

  public static final long DEEP_OVERHEAD_CAM = DEEP_OVERHEAD + ClassSize.CELL_ARRAY_MAP;

  /////////////////////  CONSTRUCTORS  /////////////////////
  /**------------------------------------------------------------------------
   * C-tor to be used when new CellArrayImmutableSegment is a result of compaction of a
   * list of older ImmutableSegments.
   * The given iterator returns the Cells that "survived" the compaction.
   */
  protected CellArrayImmutableSegment(CellComparator comparator, MemStoreSegmentsIterator iterator,
      MemStoreLAB memStoreLAB, int numOfCells, MemStoreCompactor.Action action) {
    super(null, comparator, memStoreLAB); // initiailize the CellSet with NULL
    incSize(0,DEEP_OVERHEAD_CAM);
    // build the new CellSet based on CellArrayMap
    CellSet cs = createCellFlatMapSet(numOfCells, iterator, action);
    this.setCellSet(null, cs);            // update the CellSet of the new Segment
  }

  /**------------------------------------------------------------------------
   * C-tor to be used when new CellChunkImmutableSegment is built as a result of flattening
   * of CSLMImmutableSegment
   * The given iterator returns the Cells that "survived" the compaction.
   */
  protected CellArrayImmutableSegment(CSLMImmutableSegment segment, MemstoreSize memstoreSize) {
    super(segment); // initiailize the upper class
    int numOfCells = segment.getCellsCount();
    // build the new CellSet based on CellChunkMap
    CellSet cs = recreateCellFlatMapSet(numOfCells, segment.getScanner(Long.MAX_VALUE));
    this.setCellSet(segment.getCellSet(), cs);            // update the CellSet of the new Segment

    // arrange the meta-data size, decrease all meta-data sizes related to SkipList;
    // if flattening is to CellChunkMap decrease also Cell object sizes
    // (recreateCellArrayMapSet doesn't take the care for the sizes)
    long newSegmentSizeDelta = -(numOfCells * ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);
    // add size of CellArrayMap entry or CellChunkMap entry
    newSegmentSizeDelta = newSegmentSizeDelta + (numOfCells * ClassSize.CELL_ARRAY_MAP_ENTRY);
    incSize(0, newSegmentSizeDelta);
    memstoreSize.incMemstoreSize(0, newSegmentSizeDelta);
  }

  @Override
  protected long indexEntrySize() {
    return ClassSize.CELL_ARRAY_MAP_ENTRY;
  }

  @Override protected boolean isFlat() {
    return true;
  }

  /////////////////////  PRIVATE METHODS  /////////////////////
  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellArrayMap from compacting iterator
  protected CellSet createCellFlatMapSet(int numOfCells, MemStoreSegmentsIterator iterator,
      MemStoreCompactor.Action action) {

    Cell[] cells = new Cell[numOfCells];   // build the Cell Array
    int i = 0;
    while (iterator.hasNext()) {
      Cell c = iterator.next();
      // The scanner behind the iterator is doing all the elimination logic
      if (action == MemStoreCompactor.Action.MERGE) {
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
  // Create CellSet based on CellChunkMap from current ConcurrentSkipListMap based CellSet
  // (without compacting iterator)
  // We do not consider cells bigger than chunks!
  static protected CellSet recreateCellFlatMapSet(int numOfCells, KeyValueScanner segmentScanner) {
    Cell[] cells = new Cell[numOfCells];   // build the Cell Array
    Cell curCell;
    int idx = 0;

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
    CellArrayMap cam = new CellArrayMap(CellComparator.COMPARATOR, cells, 0, idx, false);
    return new CellSet(cam);
  }


}
