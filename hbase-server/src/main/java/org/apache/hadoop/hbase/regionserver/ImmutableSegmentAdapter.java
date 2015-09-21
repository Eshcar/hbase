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
 */
public class ImmutableSegmentAdapter extends ImmutableSegment {

  final private MutableSegment delegatee;

  public ImmutableSegmentAdapter(MutableSegment delegatee) {
    this.delegatee = delegatee;
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

  @Override public long add(Cell e) {
    return delegatee.add(e);
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

  @Override public CellSet getCellSet() {
    return delegatee.getCellSet();
  }

  @Override public void dump(Log log) {
    delegatee.dump(log);
  }
}
