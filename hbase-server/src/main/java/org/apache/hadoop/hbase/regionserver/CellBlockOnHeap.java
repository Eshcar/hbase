/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Cellersion 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY CellIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;

import java.util.*;

/**
 * CellBlockOnHeap is a simple array of Cells allocated using JVM.
 * As all java arrays it is array of references pointing to Cell objects
 */
public class CellBlockOnHeap extends CellBlock {

  Cell block[];

  /* The Cells Array is created only when CellBlockOnHeap is created, all sub-CellBlocks use
   * boundary indexes */
  public CellBlockOnHeap(Comparator<? super Cell> comparator, Cell b[], int min, int max) {
    super(comparator,min,max);
    this.block = b;
  }

  /* To be used by base class only to create a sub-CellBlock */
  @Override
  protected CellBlock createCellBlocks(Comparator<? super Cell> comparator, int min, int max) {
    return new CellBlockOnHeap(comparator,this.block,min,max);
  }

  @Override
  protected Cell getCellFromIndex(int i) {
    return block[i];
  }
}
