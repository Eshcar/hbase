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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;

/**
 * CellBlocks stores a constant number of elements and is immutable after creation stage.
 * Due to being immutable the CellBlocks can be implemented as array.
 * The CellBlocks uses no synchronization primitives, it is assumed to be created by a
 * single thread and then it can be read-only by multiple threads.
 */
public class CellBlocksOffHeap extends CellBlocks {

  byte block[];
  private final Configuration conf;

  public CellBlocksOffHeap(Comparator<? super Cell> comparator, Configuration conf,
      byte b[], int min, int max, boolean d) {
    super(comparator,min,max,d);
    this.block = b;
    this.conf = conf;
  }

  @Override
  protected CellBlocks createCellBlocksMap(Comparator<? super Cell> comparator, int min, int max,
      boolean d) {
    return new CellBlocksOffHeap(comparator, this.conf, this.block, min, max, d);
  }

  @Override
  protected Cell getCellFromIndex(int i) {
    int offsetInBytes = 3*i*(Integer.SIZE / Byte.SIZE);
    int chunkId = Bytes.toInt(block,offsetInBytes);
    int offsetOfCell = Bytes.toInt(block,offsetInBytes+3*(Integer.SIZE / Byte.SIZE));
    int lengthOfCell = Bytes.toInt(block,offsetInBytes+6*(Integer.SIZE / Byte.SIZE));
    byte[] chunk = MemStoreChunkPool.getPool(conf).translateIdToChunk(chunkId).getData();
    Cell result = new KeyValue(chunk, offsetOfCell, lengthOfCell);
    return result;
  }
}
