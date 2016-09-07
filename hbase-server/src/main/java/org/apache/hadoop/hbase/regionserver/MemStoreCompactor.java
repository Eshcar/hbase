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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.io.IOException;
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

  // The external setting of the compacting MemStore behaviour
  // Compaction of the index without the data is the default
  static final String COMPACTING_MEMSTORE_TYPE_KEY = "hbase.hregion.compacting.memstore.type";
  static final String COMPACTING_MEMSTORE_TYPE_DEFAULT = "index-compaction";

  // What percentage of the duplications is causing compaction?
  static final String COMPACTION_THRESHOLD_REMAIN_FRACTION
      = "hbase.hregion.compacting.memstore.comactPercent";
  static final double COMPACTION_THRESHOLD_REMAIN_FRACTION_DEFAULT = 0.3;

  // Option for external guidance whether the speculative scan
  // (to evaluate compaction efficiency) is allowed
  static final String MEMSTORE_COMPACTOR_AVOID_SPECULATIVE_SCAN
      = "hbase.hregion.compacting.memstore.avoidSpeculativeScan";
  static final boolean MEMSTORE_COMPACTOR_AVOID_SPECULATIVE_SCAN_DEFAULT = false;

  // Maximal number of the segments in the compaction pipeline
  private static final int THRESHOLD_PIPELINE_SEGMENTS = 3;

  private static final Log LOG = LogFactory.getLog(MemStoreCompactor.class);
  private CompactingMemStore compactingMemStore;

  // a static version of the segment list from the pipeline
  private VersionedSegmentsList versionedList;

  // a flag raised when compaction is requested to stop
  private final AtomicBoolean isInterrupted = new AtomicBoolean(false);

  // the limit to the size of the groups to be later provided to MemStoreCompactorIterator
  private final int compactionKVMax;

  double fraction = 0.7;

  int immutCellsNum = 0;  // number of immutable for compaction cells

  /**
   * Types of actions to be done on the pipeline upon MemStoreCompaction invocation
   */
  private enum Action {
    NOP,
    FLATTEN,
    MERGE,
    COMPACT
  }

  private Action action = Action.FLATTEN;

  public MemStoreCompactor(CompactingMemStore compactingMemStore) {
    this.compactingMemStore = compactingMemStore;
    this.compactionKVMax = compactingMemStore.getConfiguration()
        .getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);
    this.fraction = 1 - compactingMemStore.getConfiguration().getDouble(
        COMPACTION_THRESHOLD_REMAIN_FRACTION,
        COMPACTION_THRESHOLD_REMAIN_FRACTION_DEFAULT);
  }

  /**----------------------------------------------------------------------
   * The request to dispatch the compaction asynchronous task.
   * The method returns true if compaction was successfully dispatched, or false if there
   * is already an ongoing compaction or no segments to compact.
   */
  public boolean start() throws IOException {
    if (!compactingMemStore.hasImmutableSegments()) // no compaction on empty pipeline
      return false;

    String memStoreType = compactingMemStore.getConfiguration().get(COMPACTING_MEMSTORE_TYPE_KEY,
        COMPACTING_MEMSTORE_TYPE_DEFAULT);

    switch (memStoreType) {
      case "index-compaction": action = Action.MERGE;
        break;
      case "data-compaction": action = Action.COMPACT;
        break;
      default:
        throw new RuntimeException("Unknown action " + action); // sanity check
    }

    // get a snapshot of the list of the segments from the pipeline,
    // this local copy of the list is marked with specific version
    versionedList = compactingMemStore.getImmutableSegments();
    immutCellsNum = versionedList.getNumOfCells();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting the In-Memory Compaction for store "
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
   * Check whether there are some signs to definitely not to flatten,
   * returns false if we must compact. If this method returns true we
   * still need to evaluate the compaction.
   */
  private Action policy() {

    if (isInterrupted.get())        // if the entire process is interrupted cancel flattening
      return Action.NOP;            // the compaction also doesn't start when interrupted

    if (action == Action.COMPACT) { // try to compact if it worth it
      // check if running the speculative scan (to check the efficiency of compaction)
      // is allowed by the user
      boolean avoidSpeculativeScan =
          compactingMemStore.getConfiguration().getBoolean(
              MEMSTORE_COMPACTOR_AVOID_SPECULATIVE_SCAN,
              MEMSTORE_COMPACTOR_AVOID_SPECULATIVE_SCAN_DEFAULT);
      if (avoidSpeculativeScan==true) {
        LOG.debug("In-Memory Compaction Pipeline for store " + compactingMemStore.getFamilyName()
            + " is going to be compacted, without compaction-evaluation");
        return Action.COMPACT;      // compact without checking the compaction expedience
      }
      if (worthDoingCompaction()) {
        return Action.COMPACT;      // compact because it worth it
      }
    }

    // compaction shouldn't happen or doesn't worth it
    // limit the number of the segments in the pipeline
    int numOfSegments = versionedList.getNumOfSegments();
    if (numOfSegments > THRESHOLD_PIPELINE_SEGMENTS) {
      LOG.debug("In-Memory Compaction Pipeline for store " + compactingMemStore.getFamilyName()
          + " is going to be merged, as there are " + numOfSegments + " segments");
      action = Action.MERGE;
      return Action.MERGE;          // to avoid too many segments, merge now
    }

    // if nothing of the above, then just flatten the newly joined segment
    LOG.debug("The youngest segment in the in-Memory Compaction Pipeline for store "
        + compactingMemStore.getFamilyName() + " is going to be flattened");
    return Action.FLATTEN;
  }

  /**----------------------------------------------------------------------
  * The worker thread performs the compaction asynchronously.
  * The solo (per compactor) thread only reads the compaction pipeline.
  * There is at most one thread per memstore instance.
  */
  private void doCompaction() {
    ImmutableSegment result = null;
    boolean resultSwapped = false;

    try {
      Action nextStep = policy();
      switch (nextStep){
      case FLATTEN:   // Youngest Segment in the pipeline is with SkipList index, make it flat
        compactingMemStore.flattenOneSegment(versionedList.getVersion());
      case NOP:       // intentionally falling through
        return;
      case MERGE:
      case COMPACT:
        break;
      default: throw new RuntimeException("Unknown action " + action); // sanity check
      }

      // Create one segment representing all segments in the compaction pipeline,
      // either by compaction or by merge
      if (!isInterrupted.get()) {
        result = createSubstitution();
      }

      // Substitute the pipeline with one segment
      if (!isInterrupted.get()) {
        if (resultSwapped = compactingMemStore.swapCompactedSegments(versionedList, result)) {
          // update the wal so it can be truncated and not get too long
          compactingMemStore.updateLowestUnflushedSequenceIdInWAL(true); // only if greater
        }
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
   * Creation of the ImmutableSegment either by merge or copy-compact of the segments of the
   * pipeline, based on the Compactor Iterator. The new ImmutableSegment is returned.
   */
  private ImmutableSegment createSubstitution() throws IOException {

    ImmutableSegment result = null;
    MemStoreCompactorIterator iterator =
        new MemStoreCompactorIterator(versionedList.getStoreSegments(),
            compactingMemStore.getComparator(),
            compactionKVMax, compactingMemStore.getStore());
    try {
      switch (action) {
      case COMPACT:
        result = SegmentFactory.instance().createImmutableSegment(
            compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator,
            immutCellsNum, ImmutableSegment.Type.ARRAY_MAP_BASED);
        break;
      case MERGE:
        result = SegmentFactory.instance().createImmutableSegment(
            compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator,
            immutCellsNum, ImmutableSegment.Type.ARRAY_MAP_BASED, versionedList.getStoreSegments());
        break;
      default: throw new RuntimeException("Unknown action " + action); // sanity check
      }
    } finally {
      iterator.close();
    }

    return result;
  }

  /**----------------------------------------------------------------------
   * Estimate the efficiency of the future compaction
   */
  private boolean worthDoingCompaction() {

    int cnt = 0;
    MemStoreCompactorIterator iterator = null;

    try {
      iterator =
        new MemStoreCompactorIterator(
            versionedList.getStoreSegments(), compactingMemStore.getComparator(),
            compactionKVMax, compactingMemStore.getStore());

      while (iterator.next() != null) {
        cnt++;
      }
    } catch(Exception e) {
      return false;
    } finally {
      if (iterator!=null) iterator.close();
    }

    if (cnt >= fraction * versionedList.getNumOfCells())
      return false;

    LOG.debug("In-Memory Compaction Pipeline for store " + compactingMemStore.getFamilyName()
        + " is going to be compacted, according to compaction-evaluation, " + cnt
        + " cells will remain out of " + versionedList.getNumOfCells());
    immutCellsNum = cnt;
    return true ;
  }
}
