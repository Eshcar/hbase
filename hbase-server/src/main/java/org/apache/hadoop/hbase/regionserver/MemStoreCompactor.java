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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The ongoing MemStore Compaction manager, dispatches a solo running compaction
 * and interrupts the compaction if requested.
 * The MemStoreScanner is used to traverse the compaction pipeline. The MemStoreScanner
 * is included in internal store scanner, where all compaction logic is implemented.
 * Threads safety: It is assumed that the compaction pipeline is immutable,
 * therefore no special synchronization is required.
 */
@InterfaceAudience.Private
class MemStoreCompactor {
  private static final Log LOG = LogFactory.getLog(MemStoreCompactor.class);

  private CompactionPipeline pipeline;        // the subject for compaction
  private CompactingMemStore ms;              // backward reference
  private MemStoreScanner scanner;            // scanner for pipeline only
  private Store store;

  // scanner on top of MemStoreScanner that uses ScanQueryMatcher
  private StoreScanner compactingScanner;
  private Configuration conf;

  // smallest read point for any ongoing MemStore scan
  private long smallestReadPoint;

  // a static version of the CellSetMgrs list from the pipeline
  private VersionedSegmentsList versionedList;
  private final CellComparator comparator;
  private final AtomicBoolean isInterrupted = new AtomicBoolean(false);

  /**
   * ----------------------------------------------------------------------
   * The constructor is used only to initialize basics, other parameters
   * needing to start compaction will come with startCompact()
   */
  public MemStoreCompactor(CompactingMemStore ms, CompactionPipeline pipeline,
      CellComparator comparator, Store store, Configuration conf) {

    this.ms = ms;
    this.pipeline = pipeline;
    this.comparator = comparator;
    this.conf  = conf;
    this.store = store;
  }

  /**
   * ----------------------------------------------------------------------
   * The request to dispatch the compaction asynchronous task.
   * The method returns true if compaction was successfully dispatched, or false if there
   *
   * is already an ongoing compaction (or pipeline is empty).
   */
  public boolean startCompact() throws IOException {
    if (pipeline.isEmpty()) return false;             // no compaction on empty pipeline

    List<StoreSegmentScanner> scanners = new ArrayList<StoreSegmentScanner>();
    this.versionedList = pipeline.getVersionedList(); // get the list of segments from the pipeline
                                                      // the list is marked with specific version

    // create the list of scanners with maximally possible read point, meaning that
    // all KVs are going to be returned by the pipeline traversing
    for (StoreSegment segment : this.versionedList.getStoreSegments()) {
        scanners.add(segment.getScanner(Long.MAX_VALUE));
    }
    scanner =
          new MemStoreScanner(ms, scanners, Long.MAX_VALUE, MemStoreScanner.Type.COMPACT_FORWARD);

    smallestReadPoint = store.getSmallestReadPoint();
    compactingScanner = createScanner(store);

    LOG.info("Starting the MemStore in-memory compaction for store " + store.getColumnFamilyName());

    doCompact();
    return true;
  }

  /*----------------------------------------------------------------------
  * The request to cancel the compaction asynchronous task
  * The compaction may still happen if the request was sent too late
  * Non-blocking request
  */
  public void stopCompact() {
    isInterrupted.compareAndSet(false, true);
  }


  /*----------------------------------------------------------------------
  * Close the scanners and clear the pointers in order to allow good
  * garbage collection
  */
  private void releaseResources() {
    isInterrupted.set(false);
    scanner.close();
    scanner = null;
    compactingScanner.close();
    compactingScanner = null;
    versionedList = null;
  }

  /*----------------------------------------------------------------------
  * The worker thread performs the compaction asynchronously.
  * The solo (per compactor) thread only reads the compaction pipeline.
  * There is at most one thread per memstore instance.
  */
  private void doCompact() {

    ImmutableSegment result = StoreSegmentFactory.instance()  // create the scanner
        .createImmutableSegment(conf, comparator,
            CompactingMemStore.DEEP_OVERHEAD_PER_PIPELINE_ITEM);

    // the compaction processing
    try {
      // Phase I: create the compacted MutableCellSetSegment
      compactSegments(result);

      // Phase II: swap the old compaction pipeline
      if (!isInterrupted.get()) {
        pipeline.swap(versionedList, result);
        // update the wal so it can be truncated and not get too long
        ms.updateLowestUnflushedSequenceIdInWal(true); // only if greater
      }
    } catch (Exception e) {
        LOG.debug("Interrupting the MemStore in-memory compaction for store " + ms.getFamilyName());
        Thread.currentThread().interrupt();
        return;
    } finally {
        releaseResources();
    }

  }

  /**
   * Creates the scanner for compacting the pipeline.
   *
   * @return the scanner
   */
  private StoreScanner createScanner(Store store) throws IOException {

    Scan scan = new Scan();
    scan.setMaxVersions();  //Get all available versions

    StoreScanner internalScanner =
        new StoreScanner(store, store.getScanInfo(), scan, Collections.singletonList(scanner),
            ScanType.COMPACT_RETAIN_DELETES, smallestReadPoint, HConstants.OLDEST_TIMESTAMP);

    return internalScanner;
  }

  /**
   * Updates the given single StoreSegment using the internal store scanner,
   * who in turn uses ScanQueryMatcher
   */
  private void compactSegments(StoreSegment result) throws IOException {

    List<Cell> kvs = new ArrayList<Cell>();
    // get the limit to the size of the groups to be returned by compactingScanner
    int compactionKVMax = conf.getInt(
        HConstants.COMPACTION_KV_MAX,
        HConstants.COMPACTION_KV_MAX_DEFAULT);

    ScannerContext scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    boolean hasMore;
    do {
      hasMore = compactingScanner.next(kvs, scannerContext);
      if (!kvs.isEmpty()) {
        for (Cell c : kvs) {
          // The scanner is doing all the elimination logic
          // now we just copy it to the new segment
          KeyValue kv = KeyValueUtil.ensureKeyValue(c);
          Cell newKV = result.maybeCloneWithAllocator(kv);
          result.add(newKV);

        }
        kvs.clear();
      }
    } while (hasMore && (!isInterrupted.get()));
  }

}
