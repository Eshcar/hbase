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
import org.apache.hadoop.hbase.KeyValue;
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
  private LinkedList<CellSetMgr> pipeline;
  private long version;
  // a lock to protect critical sections changing the structure of the list
  private final Lock lock;

  private static final CellSetMgr EMPTY_CELL_SET_MGR = CellSetMgr.Factory.instance()
      .createCellSetMgr(CellSetMgr.Type.EMPTY, null,
          CompactedMemStore.DEEP_OVERHEAD_PER_PIPELINE_ITEM);

  public CompactionPipeline(HRegion region) {
    this.region = region;
    this.pipeline = new LinkedList<CellSetMgr>();
    this.version = 0;
    this.lock = new ReentrantLock(true);
  }

  public boolean pushHead(CellSetMgr cellSetMgr) {
    lock.lock();
    try {
      return addFirst(cellSetMgr);
    } finally {
      lock.unlock();
    }
  }

  public CellSetMgr pullTail() {
    lock.lock();
    try {
      if(pipeline.isEmpty()) {
        return EMPTY_CELL_SET_MGR;
      }
      return removeLast();
    } finally {
      lock.unlock();
    }
  }

  public CellSetMgr peekTail() {
    lock.lock();
    try {
      if(pipeline.isEmpty()) {
        return EMPTY_CELL_SET_MGR;
      }
      return peekLast();
    } finally {
      lock.unlock();
    }
  }

  public VersionedCellSetMgrList getVersionedList() {
    lock.lock();
    try {
      LinkedList<CellSetMgr> cellSetMgrList = new LinkedList<CellSetMgr>(pipeline);
      VersionedCellSetMgrList res = new VersionedCellSetMgrList(cellSetMgrList, version);
      return res;
    } finally {
      lock.unlock();
    }
  }

  public boolean swap(VersionedCellSetMgrList versionedList, CellSetMgr cellSetMgr) {
    if(versionedList.getVersion() != version) {
      return false;
    }
    lock.lock();
    try {
      if(versionedList.getVersion() != version) {
        return false;
      }
      LinkedList<CellSetMgr> suffix = versionedList.getCellSetMgrList();
      boolean valid = validateSufixList(suffix);
      if(!valid) return false;
      LOG.info("Swapping pipeline suffix with compacted item.");
      swapSuffix(suffix,cellSetMgr);
      if(region != null) {
        // update the global memstore size counter
        long suffixSize = CompactedMemStore.getCellSetMgrListSize(suffix);
        long newSize = CompactedMemStore.getCellSetMgrSize(cellSetMgr);
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

  public long rollback(KeyValue cell) {
    lock.lock();
    long sz = 0;
    try {
      if(!pipeline.isEmpty()) {
        Iterator<CellSetMgr> pipelineBackwardIterator = pipeline.descendingIterator();
        CellSetMgr current = pipelineBackwardIterator.next();
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

  public void getRowKeyAtOrBefore(GetClosestRowBeforeTracker state) {
    for(CellSetMgr item : getCellSetMgrList()) {
      item.getRowKeyAtOrBefore(state);
    }
  }

  public boolean isEmpty() {
    return pipeline.isEmpty();
  }

  public LinkedList<CellSetMgr> getCellSetMgrList() {
    lock.lock();
    try {
      LinkedList<CellSetMgr> res = new LinkedList<CellSetMgr>(pipeline);
      return res;
    } finally {
      lock.unlock();
    }

  }

  public long size() {
    return pipeline.size();
  }

  private boolean validateSufixList(LinkedList<CellSetMgr> suffix) {
    if(suffix.isEmpty()) {
      // empty suffix is always valid
      return true;
    }

    Iterator<CellSetMgr> pipelineBackwardIterator = pipeline.descendingIterator();
    Iterator<CellSetMgr> suffixBackwardIterator = suffix.descendingIterator();
    CellSetMgr suffixCurrent;
    CellSetMgr pipelineCurrent;
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

  private void swapSuffix(LinkedList<CellSetMgr> suffix, CellSetMgr cellSetMgr) {
    version++;
    for(CellSetMgr itemInSuffix : suffix) {
      itemInSuffix.close();
    }
    pipeline.removeAll(suffix);
    pipeline.addLast(cellSetMgr);
  }

  private CellSetMgr removeLast() {
    version++;
    return pipeline.removeLast();
  }

  private CellSetMgr peekLast() {
    return pipeline.peekLast();
  }

  private boolean addFirst(CellSetMgr cellSetMgr) {
    pipeline.add(0,cellSetMgr);
    return true;
  }

  private void incVersion() {
    version++;
  }

}
