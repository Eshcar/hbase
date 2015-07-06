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

import java.util.LinkedList;

/**
 * A list of cell set managers coupled with the version of the memstore version at the time it was
 * created.
 * This structure helps guarantee that the how memstore organizes its data is changed in a
 * consistent (atomic) way.
 * Specifically, swapping some of the elements in a compaction pipeline with a new compacted
 * element is permitted only if the pipeline version is the same as the version attached to the
 * elements.
 *
 */
@InterfaceAudience.Private
public class VersionedCellSetMgrList {

  private final LinkedList<MemStoreSegment> cellSetMgrList;
  private final long version;

  public VersionedCellSetMgrList(
      LinkedList<MemStoreSegment> cellSetMgrList, long version) {
    this.cellSetMgrList = cellSetMgrList;
    this.version = version;
  }

  public LinkedList<MemStoreSegment> getCellSetMgrList() {
    return cellSetMgrList;
  }

  public long getVersion() {
    return version;
  }
}
