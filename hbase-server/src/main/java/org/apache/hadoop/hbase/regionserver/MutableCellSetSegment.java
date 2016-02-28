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

import java.util.Iterator;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This mutable store segment encapsulates a mutable cell set and its respective memory allocation
 * buffers (MSLAB).
 */
@InterfaceAudience.Private
final class MutableCellSetSegment extends MutableSegment {

  private volatile CellSet cellSet;
  private final CellComparator comparator;

  // Instantiate objects only using factory
  MutableCellSetSegment(CellSet cellSet, MemStoreLAB memStoreLAB, long size,
      CellComparator comparator) {
    super(memStoreLAB, size);
    this.cellSet = cellSet;
    this.comparator = comparator;
  }

  @Override
  public SegmentScanner getSegmentScanner(long readPoint) {
    return new SegmentScanner(this, readPoint);
  }

  @Override
  public long add(Cell cell) {
    return internalAdd(cell);
  }

  @Override
  public long rollback(Cell cell) {
    Cell found = get(cell);
    if (found != null && found.getSequenceId() == cell.getSequenceId()) {
      long sz = AbstractMemStore.heapSizeChange(cell, true);
      remove(cell);
      incSize(-sz);
      return sz;
    }
    return 0;
  }

  @Override
  protected CellSet getCellSet() {
    return cellSet;
  }
  @Override
  protected CellComparator getComparator() {
    return comparator;
  }

  private Cell get(Cell cell) {
    return getCellSet().get(cell);
  }

  private boolean remove(Cell e) {
    return getCellSet().remove(e);
  }

  // methods for tests
  @Override
  Cell first() {
    return this.getCellSet().first();
  }

}
