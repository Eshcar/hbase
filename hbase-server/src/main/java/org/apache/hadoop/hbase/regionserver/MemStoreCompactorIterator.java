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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.*;

/**
 * The MemStoreCompactorIterator is designed to perform one iteration over given list of segments
 * For another iteration new instance of MemStoreCompactorIterator needs to be created
 * The iterator is not thread-safe and must have only one instance in each period of time
 */
@InterfaceAudience.Private
public class MemStoreCompactorIterator implements Iterator<Cell> {

  private List<Cell> kvs = new ArrayList<Cell>();
  private int c = 0;
  // scanner for full or partial pipeline (heap of segment scanners)
  private KeyValueScanner scanner;
  // scanner on top of pipeline scanner that uses ScanQueryMatcher
  private StoreScanner compactingScanner;
  // get the limit to the size of the groups to be returned by compactingScanner
  private final int compactionKVMax;

  private final ScannerContext scannerContext;

  private boolean hasMore;
  private Iterator<Cell> kvsIterator;

  // C-tor
  public MemStoreCompactorIterator(int initiator, LinkedList<ImmutableSegment> segments,
      CellComparator comparator, int compactionKVMax, HStore store) throws IOException {
    this.compactionKVMax = compactionKVMax;
    this.scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
    //org.junit.Assert.assertTrue("\n\n<<< Initiating compactor iterator\n", false);

    // list of Scanners of segments in the pipeline, when compaction starts
    List<SegmentScanner> scanners = new ArrayList<SegmentScanner>();

    // create the list of scanners with maximally possible read point, meaning that
    // all KVs are going to be returned by the pipeline traversing
    for (Segment segment : segments) {
      scanners.add(segment.getSegmentScanner(Long.MAX_VALUE));
    }

    scanner =
        new MemStoreScanner(comparator, scanners, MemStoreScanner.Type.COMPACT_FORWARD);

    // reinitialize the compacting scanner for each instance of iterator
    compactingScanner = createScanner(store, scanner, initiator);

    hasMore = compactingScanner.next(kvs, scannerContext);
    if (!kvs.isEmpty()) {
      kvsIterator = kvs.iterator();
    }

//    if (initiator == 2)
//      org.junit.Assert.assertTrue("\n\n<<< Iterator constructing. "
//          + "Store: " + store + ", Scanner: " + scanner + ", hasMore: " + hasMore + "\n", false);
  }

  @Override
  public boolean hasNext() {
    if (!kvsIterator.hasNext()) {
      if (!refillKVS())  return false;
    }
    return hasMore;
  }

  @Override
  public Cell next()  {
    c++;
    //if (c>1) org.junit.Assert.assertTrue("\n\n<<< " + c + " iterations of iterator\n",false);
    if (!kvsIterator.hasNext()) {
      //          org.junit.Assert.assertTrue("\n\n<<< Got to empty kvs for the first time with " + c
      //              + " iterations\n", false);
      if (!refillKVS())  return null;
    }

    return (!hasMore) ? null : kvsIterator.next();
  }

  @Override
  public void remove() {
    compactingScanner.close();
    compactingScanner = null;
    scanner.close();
    scanner = null;
  }

  /**
   * Creates the scanner for compacting the pipeline.
   *
   * @return the scanner
   */
  private StoreScanner createScanner(Store store, KeyValueScanner scanner, int init)
      throws IOException {

    Scan scan = new Scan();
    scan.setMaxVersions();  //Get all available versions

//    if (init == 2)
//      org.junit.Assert.assertTrue("\n\n<<< Before creating the internal scanner " + "\n", false);

    StoreScanner internalScanner =
        new StoreScanner(store, store.getScanInfo(), scan, Collections.singletonList(scanner),
            ScanType.COMPACT_RETAIN_DELETES, store.getSmallestReadPoint(),
            HConstants.OLDEST_TIMESTAMP);

    return internalScanner;
  }

  private boolean refillKVS() {
    kvs.clear();    // clear previous KVS, first initiated in the constructor

    if (!hasMore) { // if there is nothing expected next in compactingScanner
      org.junit.Assert.assertTrue("\n\n<<< Got to empty kvs for the first time with " + c
          + " iterations and no more\n", false);
      return false;
    }

    try {           // try to get next KVS
      hasMore = compactingScanner.next(kvs, scannerContext);
    } catch (IOException ie) {
      org.junit.Assert.assertTrue("\n\n<<< GOT EXCEPTION IN ITERATOR !!!\n",false);
      throw new IllegalStateException(ie);
    }

    if (!kvs.isEmpty() ) {  // is the new KVS empty ?
      kvsIterator = kvs.iterator();
      return true;
    } else {
      //            org.junit.Assert.assertTrue("\n\n<<< Got to empty kvs for the first time with " + c
      //                + " iterations and empty set\n", false);
      return false;
    }
  }
}

