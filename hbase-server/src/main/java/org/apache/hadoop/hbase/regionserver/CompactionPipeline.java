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
 * The compaction pipeline of a {@link CompactingMemStore}, is a FIFO queue of cell set buckets.
 * It supports pushing a cell set bucket at the head of the pipeline and pulling a bucket from the
 * tail to flush to disk.
 * It also supports swap operation to allow the compactor swap a subset of the buckets with a new
 * (compacted) one. This swap succeeds only if the version number passed with the list of buckets
 * to swap is the same as the current version of the pipeline.
 * The pipeline version is updated whenever swapping buckets or pulling the bucket at the tail.
 */
@InterfaceAudience.Private
class CompactionPipeline {
  private static final Log LOG = LogFactory.getLog(CompactionPipeline.class);

  private final HRegion region;
  private LinkedList<ImmutableSegment> pipeline;
  private long version;
  // a lock to protect critical sections changing the structure of the list
  private final Lock lock;

  private static final ImmutableSegment EMPTY_MEM_STORE_SEGMENT = StoreSegmentFactory.instance()
      .createImmutableSegment(null,
          CompactingMemStore.DEEP_OVERHEAD_PER_PIPELINE_ITEM);

  public CompactionPipeline(HRegion region) {
    this.region = region;
    this.pipeline = new LinkedList<ImmutableSegment>();
    this.version = 0;
    this.lock = new ReentrantLock(true);
  }

  public boolean pushHead(MutableSegment segment) {
    ImmutableSegment immutableSegment = StoreSegmentFactory.instance().
        createImmutableSegment(region.getBaseConf(), segment);
    lock.lock();
    try {
      return addFirst(immutableSegment);
    } finally {
      lock.unlock();
    }
  }

  public ImmutableSegment pullTail() {
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
      LinkedList<ImmutableSegment> segmentList = new LinkedList<ImmutableSegment>(pipeline);
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
  public boolean swap(VersionedSegmentsList versionedList, ImmutableSegment segment) {
    if(versionedList.getVersion() != version) {
      return false;
    }
    lock.lock();
    try {
      if(versionedList.getVersion() != version) {
        return false;
      }
      LinkedList<ImmutableSegment> suffix = versionedList.getStoreSegments();
      boolean valid = validateSuffixList(suffix);
      if(!valid) return false;
      LOG.info("Swapping pipeline suffix with compacted item. "
          +"Just before the swap the number of segments in pipeline is:"
          +versionedList.getStoreSegments().size()
          +", and the number of cells in new segment is:"+segment.getCellsCount());
      swapSuffix(suffix,segment);
      if(region != null) {
        // update the global memstore size counter
        long suffixSize = CompactingMemStore.getStoreSegmentListSize(suffix);
        long newSize = CompactingMemStore.getStoreSegmentSize(segment);
        long delta = suffixSize - newSize;
        long globalMemstoreAdditionalSize = region.addAndGetGlobalMemstoreOverflowSize(-delta);
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
        Iterator<ImmutableSegment> pipelineBackwardIterator = pipeline.descendingIterator();
        StoreSegment current = pipelineBackwardIterator.next();
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

  public boolean isEmpty() {
    return pipeline.isEmpty();
  }

  public LinkedList<StoreSegment> getStoreSegmentList() {
    lock.lock();
    try {
      LinkedList<StoreSegment> res = new LinkedList<StoreSegment>(pipeline);
      return res;
    } finally {
      lock.unlock();
    }

  }

  public long size() {
    return pipeline.size();
  }

  private boolean validateSuffixList(LinkedList<ImmutableSegment> suffix) {
    if(suffix.isEmpty()) {
      // empty suffix is always valid
      return true;
    }

    Iterator<ImmutableSegment> pipelineBackwardIterator = pipeline.descendingIterator();
    Iterator<ImmutableSegment> suffixBackwardIterator = suffix.descendingIterator();
    ImmutableSegment suffixCurrent;
    ImmutableSegment pipelineCurrent;
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

  private void swapSuffix(LinkedList<ImmutableSegment> suffix, ImmutableSegment segment) {
    version++;
    for(StoreSegment itemInSuffix : suffix) {
      itemInSuffix.close();
    }
    pipeline.removeAll(suffix);
    pipeline.addLast(segment);
  }

  private ImmutableSegment removeLast() {
    version++;
    return pipeline.removeLast();
  }

  private boolean addFirst(ImmutableSegment segment) {
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
