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

import java.util.Comparator;
import org.apache.hadoop.hbase.Cell;

/**
 * CellArrayMap is a simple array of Cells and can be allocated only using JVM.
 * In contrast, CellChunkMap can be also allocated off-heap.
 * As all java arrays CellArrayMap's array of references pointing to Cell objects.
 */
public class CellArrayMap extends CellFlatMap {

  private final Cell[] block;

  /* The Cells Array is created only when CellArrayMap is created, all sub-CellBlocks use
   * boundary indexes. The given Cell array must be ordered. */
  public CellArrayMap(Comparator<? super Cell> comparator, Cell[] b, int min, int max, boolean d) {
    super(comparator,min,max,d);
    this.block = b;
  }

  /* To be used by base class only to create a sub-CellFlatMap */
  @Override
  protected CellFlatMap createCellFlatMap(Comparator<? super Cell> comparator, int min, int max,
      boolean d) {
    return new CellArrayMap(comparator,this.block,min,max,d);
  }

  @Override
  protected Cell getCell(int i) {
    return block[i];
  }
}
