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

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.util.concurrent.atomic.AtomicInteger;

public class MagicCompactionStrategy extends MemStoreCompactionStrategy{

  private static final String name = "MAGIC";
  public static final String MAGIC_COMPACTION_THRESHOLD_KEY =
      "hbase.hregion.magic.compaction.threshold";
  private static final double MAGIC_COMPACTION_THRESHOLD_DEFAULT = 0.3;

  private double compactionThreshold;

  // duplication info
  private int maxNumCellsInMemory;
  private AtomicInteger numCellsInMemory = new AtomicInteger(0);
  private AtomicInteger numDuplicateCells = new AtomicInteger(0);
  private BloomFilter rowBF;
  private BloomFilter rowcolBF;

  public MagicCompactionStrategy(Configuration conf, String cfName) {
    super(conf, cfName);
    compactionThreshold = conf.getDouble(MAGIC_COMPACTION_THRESHOLD_KEY,
        MAGIC_COMPACTION_THRESHOLD_DEFAULT);
    maxNumCellsInMemory = (int) HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE / 10000;
    resetDuplicationInfo();
  }

  @Override public Action getAction(VersionedSegmentsList versionedList) {
    if((double)numDuplicateCells.get()/numCellsInMemory.get() > compactionThreshold) {
      return Action.COMPACT;
    }
    return simpleMergeOrFlatten(versionedList, name);
  }

  @Override
  public void resetDuplicationInfo() {
    rowBF = createMemstoreBloomFilter(BloomType.ROW);
    rowcolBF = createMemstoreBloomFilter(BloomType.ROWCOL);
  }

  @Override
  public void updateDuplicationInfo(VersionedSegmentsList versionedList, ImmutableSegment result) {
    int numCellsBefore = versionedList.getNumOfCells();
    int numCellsAfter = 0;
    if(result != null) {
      numCellsAfter = result.getCellsCount();
    }
    int delta = numCellsAfter - numCellsBefore;
    int num = numCellsInMemory.addAndGet(delta);
    if(num<0) {
      numCellsInMemory.set(0);
    }
    num = numDuplicateCells.addAndGet(delta);
    if(num<0) {
      numDuplicateCells.set(0);
    }
  }

  @Override
  public void updateDuplicationInfo(Cell cell) {
    int sizeOfKey = CellUtil.estimatedSerializedSizeOfKey(cell);
    byte[] key = new byte[sizeOfKey];
    int offset = CellUtil.copyRowTo(cell, key, 0);
    if(rowBF.mightContain(key)) {
      offset = CellUtil.copyFamilyTo(cell, key, offset);
      CellUtil.copyQualifierTo(cell, key, offset);
      if(rowcolBF.mightContain(key)){
        numDuplicateCells.incrementAndGet();
      } else {
        rowcolBF.put(key);
      }
    } else {
      rowBF.put(key);
      offset = CellUtil.copyFamilyTo(cell, key, offset);
      CellUtil.copyQualifierTo(cell, key, offset);
      rowcolBF.put(key);
    }
    int num = numCellsInMemory.incrementAndGet();
    if(num > maxNumCellsInMemory) {
      maxNumCellsInMemory = num;
    }
  }

  private BloomFilter createMemstoreBloomFilter(BloomType bloomType) {
    float err = 0.01f;
    int numKeys = maxNumCellsInMemory;
    if (bloomType == BloomType.ROW) {
      //      err = (float) (1 - Math.sqrt(1 - err));
      numKeys /= 10;
    }
    // In case of compound Bloom filters we ignore the maxKeys hint.
    BloomFilter bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), numKeys, err);
    return bloomFilter;
  }

}
