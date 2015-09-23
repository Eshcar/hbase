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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;

/**
 * An immutable memstore segment which wraps and adapts a mutable segment.
 * This is used when a mutable segment is being pushed into a compaction pipeline,
 * that consists only of immutable segments.
 * The compaction may generate different type of mutable segment
 */
public class ImmutableSegmentAdapter extends ImmutableSegment {

  final private MutableSegment delegatee;

  public ImmutableSegmentAdapter(MutableSegment segment) {
    super(segment);
    this.delegatee = segment;
  }

  @Override
  public KeyValueScanner getScannerForMemStoreSnapshot() {
    return new CollectionBackedScanner(delegatee.getCellSet(),delegatee.getComparator());
  }

  @Override public StoreSegmentScanner getScanner(long readPoint) {
    return delegatee.getScanner(readPoint);
  }

  @Override public boolean isEmpty() {
    return delegatee.isEmpty();
  }

  @Override public int getCellsCount() {
    return delegatee.getCellsCount();
  }

  @Override public long add(Cell cell) {
    return delegatee.add(cell);
  }

  @Override public Cell getFirstAfter(Cell cell) {
    return delegatee.getFirstAfter(cell);
  }

  @Override public void close() {
    delegatee.close();
  }

  @Override public Cell maybeCloneWithAllocator(Cell cell) {
    return delegatee.maybeCloneWithAllocator(cell);
  }

  @Override public StoreSegment setSize(long size) {
    delegatee.setSize(size);
    return this;
  }

  @Override public long getSize() {
    return delegatee.getSize();
  }

  @Override public long rollback(Cell cell) {
    return delegatee.rollback(cell);
  }

  @Override public CellSet getCellSet() {
    return delegatee.getCellSet();
  }

  @Override public void dump(Log log) {
    delegatee.dump(log);
  }
}
