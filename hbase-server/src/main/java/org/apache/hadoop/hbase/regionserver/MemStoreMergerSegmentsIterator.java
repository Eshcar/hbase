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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The MemStoreMergerSegmentsIterator extends MemStoreSegmentsIterator
 * and performs the scan for simple merge operation meaning it is NOT based on SQM
 */
@InterfaceAudience.Private
public class MemStoreMergerSegmentsIterator extends MemStoreSegmentsIterator {

  // scanner for full or partial pipeline (heap of segment scanners)
  // we need to keep those scanners in order to close them at the end
//  protected MemStoreScanner scanner;

  // heap of scanners, lazily initialized
  private KeyValueHeap heap = null;
  // remember the initial version of the scanners list
  List<KeyValueScanner> scanners  = new ArrayList<KeyValueScanner>();

  private boolean closed = false;

  // C-tor
  public MemStoreMergerSegmentsIterator(List<ImmutableSegment> segments, CellComparator comparator,
      int compactionKVMax) throws IOException {
    super(compactionKVMax);
//    // list of Scanners of segments in the pipeline, when compaction starts
//    List<KeyValueScanner> scanners;
    // create the list of scanners to traverse over all the data
    // no dirty reads here as these are immutable segments
    int order = segments.size();
    for (ImmutableSegment segment : segments) {
      scanners.add(segment.getScanner(Integer.MAX_VALUE, order));
      order--;
    }
    heap = new KeyValueHeap(scanners, comparator);
//    scanner = new MemStoreScanner(comparator, scanners, true);
  }

  @Override
  public boolean hasNext() {
//    return (scanner.peek()!=null);
    if (closed) {
      return false;
    }
    if (this.heap != null) {
      return (this.heap.peek() != null);
    }
    // Doing this way in case some test cases tries to peek directly
    return false;
  }

  @Override
  public Cell next()  {
    Cell result = null;
    try {                 // try to get next
//      result = scanner.next();
      if (!closed && heap != null) {
        // loop over till the next suitable value
        // take next value from the heap
        for (Cell currentCell = heap.next();
             currentCell != null;
             currentCell = heap.next()) {
          // all the logic of presenting cells is inside the internal KeyValueScanners
          // located inside the heap
          result = currentCell;
        }
      }
    } catch (IOException ie) {
      throw new IllegalStateException(ie);
    }
    return result;
  }

  public void close() {
    if (closed) {
      return;
    }
    // Ensuring that all the segment scanners are closed
    if (heap != null) {
      heap.close();
      // It is safe to do close as no new calls will be made to this scanner.
      heap = null;
    } else {
      for (KeyValueScanner scanner : scanners) {
        scanner.close();
      }
    }
    closed = true;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}