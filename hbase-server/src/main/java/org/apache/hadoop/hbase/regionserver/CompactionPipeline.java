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
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The compaction pipeline of a {@link CompactedMemStore}, is a FIFO queue of cell set buckets.
 * It supports pushing a cell set bucket at the head of the pipeline and pulling a bucket from the
 * tail to flush to disk.
 * It also supports swap operation to allow the compactor swap a subset of the buckets with a new
 * (compacted) one. This swap succeeds only if the version number passed with the list of buckets
 * to swap is the same as the current version of the pipeline.
 * The pipeline version is updated whenever swapping buckets or pulling the bucket at the tail.
 */
@InterfaceAudience.Private
public class CompactionPipeline {
  private static final Log LOG = LogFactory.getLog(CompactedMemStore.class);

  private final HRegion region;
  private LinkedList<MemStoreSegment> pipeline;
  private long version;
  // a lock to protect critical sections changing the structure of the list
  private final Lock lock;

  private static final MemStoreSegment EMPTY_MEM_STORE_SEGMENT = MemStoreSegment.Factory.instance()
      .createMemStoreSegment(CellSet.Type.EMPTY, null,
          CompactedMemStore.DEEP_OVERHEAD_PER_PIPELINE_ITEM);

  public CompactionPipeline(HRegion region) {
    this.region = region;
    this.pipeline = new LinkedList<MemStoreSegment>();
    this.version = 0;
    this.lock = new ReentrantLock(true);
  }

  public boolean pushHead(MemStoreSegment segment) {
    lock.lock();
    try {
      return addFirst(segment);
    } finally {
      lock.unlock();
    }
  }

  public MemStoreSegment pullTail() {
    lock.lock();
    try {
      if(pipeline.isEmpty()) {
        return EMPTY_MEM_STORE_SEGMENT;
      }
      return removeLast();
    } finally {
      lock.unlock();
    }
  }

  public VersionedSegmentsList getVersionedList() {
    lock.lock();
    try {
      LinkedList<MemStoreSegment> segmentList = new LinkedList<MemStoreSegment>(pipeline);
      VersionedSegmentsList res = new VersionedSegmentsList(segmentList, version);
      return res;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Swaps the versioned list at the tail of the pipeline with the new compacted segment.
   * Swapping only if there were no changes to the suffix of the list while it was compacted.
   * @param versionedList tail of the pipeline that was compacted
   * @param segment new compacted segment
   * @return true iff swapped tail with new compacted segment
   */
  public boolean swap(VersionedSegmentsList versionedList, MemStoreSegment segment) {
    if(versionedList.getVersion() != version) {
      return false;
    }
    lock.lock();
    try {
      if(versionedList.getVersion() != version) {
        return false;
      }
      LinkedList<MemStoreSegment> suffix = versionedList.getMemStoreSegments();
      boolean valid = validateSuffixList(suffix);
      if(!valid) return false;
      LOG.info("Swapping pipeline suffix with compacted item.");
      swapSuffix(suffix,segment);
      if(region != null) {
        // update the global memstore size counter
        long suffixSize = CompactedMemStore.getMemStoreSegmentListSize(suffix);
        long newSize = CompactedMemStore.getMemStoreSegmentSize(segment);
        long delta = suffixSize - newSize;
        long globalMemstoreAdditionalSize = region.addAndGetGlobalMemstoreAdditionalSize(-delta);
        LOG.info("Suffix size: "+ suffixSize+" compacted item size: "+newSize+
            " globalMemstoreAdditionalSize: "+globalMemstoreAdditionalSize);
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  public long rollback(Cell cell) {
    lock.lock();
    long sz = 0;
    try {
      if(!pipeline.isEmpty()) {
        Iterator<MemStoreSegment> pipelineBackwardIterator = pipeline.descendingIterator();
        MemStoreSegment current = pipelineBackwardIterator.next();
        for (; pipelineBackwardIterator.hasNext(); current = pipelineBackwardIterator.next()) {
          sz += current.rollback(cell);
        }
        if(sz != 0) {
          incVersion();
        }
      }
      return sz;
    } finally {
      lock.unlock();
    }
  }

<<<<<<< HEAD
=======
//  public void getRowKeyAtOrBefore(GetClosestRowBeforeTracker state) {
//    for(MemStoreSegment item : getCellSetMgrList()) {
//      item.getRowKeyAtOrBefore(state);
//    }
//  }

>>>>>>> 4c61099a3ba31b1f5f89743afe4f335239b6fa0d
  public boolean isEmpty() {
    return pipeline.isEmpty();
  }

  public LinkedList<MemStoreSegment> getCellSetMgrList() {
    lock.lock();
    try {
      LinkedList<MemStoreSegment> res = new LinkedList<MemStoreSegment>(pipeline);
      return res;
    } finally {
      lock.unlock();
    }

  }

  public long size() {
    return pipeline.size();
  }

  private boolean validateSuffixList(LinkedList<MemStoreSegment> suffix) {
    if(suffix.isEmpty()) {
      // empty suffix is always valid
      return true;
    }

    Iterator<MemStoreSegment> pipelineBackwardIterator = pipeline.descendingIterator();
    Iterator<MemStoreSegment> suffixBackwardIterator = suffix.descendingIterator();
    MemStoreSegment suffixCurrent;
    MemStoreSegment pipelineCurrent;
    for( ; suffixBackwardIterator.hasNext(); ) {
      if(!pipelineBackwardIterator.hasNext()) {
        // a suffix longer than pipeline is invalid
        return false;
      }
      suffixCurrent = suffixBackwardIterator.next();
      pipelineCurrent = pipelineBackwardIterator.next();
      if(suffixCurrent != pipelineCurrent) {
        // non-matching suffix
        return false;
      }
    }
    // suffix matches pipeline suffix
    return true;
  }

  private void swapSuffix(LinkedList<MemStoreSegment> suffix, MemStoreSegment segment) {
    version++;
    for(MemStoreSegment itemInSuffix : suffix) {
      itemInSuffix.close();
    }
    pipeline.removeAll(suffix);
    pipeline.addLast(segment);
  }

  private MemStoreSegment removeLast() {
    version++;
    return pipeline.removeLast();
  }

  private boolean addFirst(MemStoreSegment segment) {
    pipeline.add(0,segment);
    return true;
  }

  private void incVersion() {
    version++;
  }

  public long getMinTimestamp() {
    long minTimestamp = Long.MIN_VALUE;
    if(!isEmpty()) {
      minTimestamp = pipeline.getLast().getMinTimestamp();
    }
    return minTimestamp;
  }
}
