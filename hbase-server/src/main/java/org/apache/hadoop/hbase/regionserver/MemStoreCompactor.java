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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The ongoing MemStore Compaction manager, dispatches a solo running compaction and interrupts
 * the compaction if requested. The compaction is interrupted and stopped by CompactingMemStore,
 * for example when another compaction needs to be started.
 * Prior to compaction the MemStoreCompactor evaluates
 * the compacting ratio and aborts the compaction if it is not worthy.
 * The MemStoreScanner is used to traverse the compaction pipeline. The MemStoreScanner
 * is included in internal store scanner, where all compaction logic is implemented.
 * Threads safety: It is assumed that the compaction pipeline is immutable,
 * therefore no special synchronization is required.
 */
@InterfaceAudience.Private
class MemStoreCompactor {

  // Possibility for external guidance whether to flatten the segments without compaction
  static final String MEMSTORE_COMPACTOR_FLATTENING = "hbase.hregion.compacting.memstore.flatten";
  static final boolean MEMSTORE_COMPACTOR_FLATTENING_DEFAULT = false;

  // Possibility for external setting of the compacted structure (SkipList, CellArray, etc.)
  static final String COMPACTING_MEMSTORE_TYPE_KEY = "hbase.hregion.compacting.memstore.type";
  static final int COMPACTING_MEMSTORE_TYPE_DEFAULT = 1;

  public final static double COMPACTION_THRESHOLD_REMAIN_FRACTION = 0.8;

  private static final Log LOG = LogFactory.getLog(MemStoreCompactor.class);
  private CompactingMemStore compactingMemStore;

  // a static version of the segment list from the pipeline
  private VersionedSegmentsList versionedList;

  // a flag raised when compaction is requested to stop
  private final AtomicBoolean isInterrupted = new AtomicBoolean(false);

  // the limit to the size of the groups to be later provided to MemStoreCompactorIterator
  private final int compactionKVMax;

  /**
   * Types of Compaction
   */
  public enum Type {
    COMPACT_TO_SKIPLIST_MAP,
    COMPACT_TO_ARRAY_MAP
  }

  private Type type = Type.COMPACT_TO_SKIPLIST_MAP;

  public MemStoreCompactor(CompactingMemStore compactingMemStore) {
    this.compactingMemStore = compactingMemStore;
    this.compactionKVMax = compactingMemStore.getConfiguration().getInt(
        HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);

  }

  /**----------------------------------------------------------------------
   * The request to dispatch the compaction asynchronous task.
   * The method returns true if compaction was successfully dispatched, or false if there
   * is already an ongoing compaction or no segments to compact.
   */
  public boolean start() throws IOException {
    if (!compactingMemStore.hasImmutableSegments()) return false;  // no compaction on empty

    int t = compactingMemStore.getConfiguration().getInt(COMPACTING_MEMSTORE_TYPE_KEY,
        COMPACTING_MEMSTORE_TYPE_DEFAULT);

    switch (t) {
      case 1: type = Type.COMPACT_TO_SKIPLIST_MAP;
        break;
      case 2: type = Type.COMPACT_TO_ARRAY_MAP;
        break;
      default: throw new RuntimeException("Unknown type " + type); // sanity check
    }

    // get a snapshot of the list of the segments from the pipeline,
    // this local copy of the list is marked with specific version
    versionedList = compactingMemStore.getImmutableSegments();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting the MemStore in-memory compaction of type " + type + " for store "
          + compactingMemStore.getStore().getColumnFamilyName());
    }

    doCompaction();
    return true;
  }

  /**----------------------------------------------------------------------
  * The request to cancel the compaction asynchronous task
  * The compaction may still happen if the request was sent too late
  * Non-blocking request
  */
  public void stop() {
      isInterrupted.compareAndSet(false, true);
  }

  /**----------------------------------------------------------------------
  * Close the scanners and clear the pointers in order to allow good
  * garbage collection
  */
  private void releaseResources() {
    isInterrupted.set(false);
    versionedList = null;
  }

  /**----------------------------------------------------------------------
  * The worker thread performs the compaction asynchronously.
  * The solo (per compactor) thread only reads the compaction pipeline.
  * There is at most one thread per memstore instance.
  */
  private void doCompaction() {
    ImmutableSegment result = null;
    boolean resultSwapped = false;
    int immutCellsNum = versionedList.getNumOfCells();  // number of immutable cells
    boolean toFlatten =                                 // the option to flatten or not to flatten
        compactingMemStore.getConfiguration().getBoolean(MEMSTORE_COMPACTOR_FLATTENING,
            MEMSTORE_COMPACTOR_FLATTENING_DEFAULT);

    try {
      // PHASE I: estimate the compaction expedience - EVALUATE COMPACTION
      if (toFlatten) {
        immutCellsNum = countCellsForCompaction();

        if ( !isInterrupted.get() &&
             (immutCellsNum
            > COMPACTION_THRESHOLD_REMAIN_FRACTION * versionedList.getNumOfCells())) {
          // too much cells "survive" the possible compaction, we do not want to compact!
          LOG.debug("In-Memory compaction does not pay off - storing the flattened segment"
              + " for store: " + compactingMemStore.getFamilyName());
          // Looking for Segment in the pipeline with SkipList index, to make it flat
          compactingMemStore.flattenOneSegment(versionedList.getVersion());
          return;
        }
      }

      // PHASE II: create the new compacted ImmutableSegment - START COPY-COMPACTION
      if (!isInterrupted.get()) {
        result = compact(immutCellsNum);
      }

      // Phase III: swap the old compaction pipeline - END COPY-COMPACTION
      if (!isInterrupted.get()) {
        if (resultSwapped = compactingMemStore.swapCompactedSegments(versionedList, result)) {
          // update the wal so it can be truncated and not get too long
          compactingMemStore.updateLowestUnflushedSequenceIdInWAL(true); // only if greater
        } else {
          // We just ignored the Segment 'result' and swap did not happen.
          result.close();
        }
      } else {
        // We just ignore the Segment 'result'.
        result.close();
      }
    } catch (Exception e) {
      LOG.debug("Interrupting the MemStore in-memory compaction for store "
          + compactingMemStore.getFamilyName());
      Thread.currentThread().interrupt();
    } finally {
      if ((result != null) && (!resultSwapped)) result.close();
      releaseResources();
    }

  }

  /**----------------------------------------------------------------------
   * The copy-compaction is the creation of the ImmutableSegment (from the relevant type)
   * based on the Compactor Iterator. The new ImmutableSegment is returned.
   */
  private ImmutableSegment compact(int numOfCells)
      throws IOException {

    LOG.debug("Starting in-memory compaction of type: " + type + ". Before compaction we have "
        + numOfCells + " cells in the entire compaction pipeline");

    ImmutableSegment result = null;
    MemStoreCompactorIterator iterator =
        new MemStoreCompactorIterator(versionedList.getStoreSegments(),
            compactingMemStore.getComparator(),
            compactionKVMax, compactingMemStore.getStore());
    try {
      switch (type) {
      case COMPACT_TO_SKIPLIST_MAP:
        result = SegmentFactory.instance().createImmutableSegment(
            compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator);
        break;
      case COMPACT_TO_ARRAY_MAP:
        result = SegmentFactory.instance().createImmutableSegment(
            compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator,
            numOfCells, ImmutableSegment.Type.ARRAY_MAP_BASED);
        break;
      default: throw new RuntimeException("Unknown type " + type); // sanity check
      }
    } finally {
      iterator.close();
    }

    return result;
  }

  /**----------------------------------------------------------------------
   * Count cells to estimate the efficiency of the future compaction
   */
  private int countCellsForCompaction() throws IOException {

    int cnt = 0;
    MemStoreCompactorIterator iterator =
        new MemStoreCompactorIterator(
            versionedList.getStoreSegments(), compactingMemStore.getComparator(),
            compactionKVMax, compactingMemStore.getStore());

    try {
      while (iterator.next() != null) {
        cnt++;
      }
    } finally {
      iterator.close();
    }

    return cnt;
  }
}
