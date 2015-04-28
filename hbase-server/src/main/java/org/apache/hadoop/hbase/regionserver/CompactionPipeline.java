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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * The compaction queue of a {@link CompactedMemStore}, is a kind of a FIFO of cell set buckets.
 * It supports pushing a cell set bucket at the head of the pipeline and pulling a bucket from the
 * tail to flush to disk.
 * It also supports swap operation to allow the compactor swap a subset of the buckets with a new
 * (compacted) one. This swap succeeds only if the version number passed with the list of buckets
 * to swap is the same as the current version of the pipeline.
 * The pipeline version is updated whenever swapping buckets or pulling the bucket at the tail.
 */
@InterfaceAudience.Private
public class CompactionPipeline {

  boolean pushHead(CellSetMgr cellSetMgr) {
    return false;
  }

  CellSetMgr pullTail() {
    return null;
  }

  VersionedCellSetMgrList getCellSetMgrList() {
    return null;
  }

  boolean swap(VersionedCellSetMgrList versionedList, CellSetMgr cellSetMgr) {
    return false;
  }
}
