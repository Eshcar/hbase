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

import org.apache.hadoop.conf.Configuration;

import java.util.Random;

public class MagicCompactionStrategy extends MemStoreCompactionStrategy{

  private static final String name = "MAGIC";
  public static final String MAGIC_COMPACTION_THRESHOLD_KEY =
      "hbase.hregion.magic.compaction.threshold";
  private static final double MAGIC_COMPACTION_THRESHOLD_DEFAULT = 0.5;
  public static final String MAGIC_INITIAL_COMPACTION_PROBABILITY_KEY =
      "hbase.hregion.magic.compaction.probability";
  private static final double MAGIC_INITIAL_COMPACTION_PROBABILITY_DEFAULT = 0.5;
  private static final double MAGIC_PROBABILITY_FACTOR = 1.02;

  private double compactionThreshold;
  private double initialCompactionProbability;
  private double compactionProbability;
  private Random rand = new Random();
  private int numCellsInVersionedList = 0;
  private boolean compacted = false;

  public MagicCompactionStrategy(Configuration conf, String cfName) {
    super(conf, cfName);
    compactionThreshold = conf.getDouble(MAGIC_COMPACTION_THRESHOLD_KEY,
        MAGIC_COMPACTION_THRESHOLD_DEFAULT);
    initialCompactionProbability = conf.getDouble(MAGIC_INITIAL_COMPACTION_PROBABILITY_KEY,
        MAGIC_INITIAL_COMPACTION_PROBABILITY_DEFAULT);
    compactionProbability = initialCompactionProbability;
  }

  @Override public Action getAction(VersionedSegmentsList versionedList) {
    if (versionedList.getEstimatedUniquesFrac() < 1.0 - compactionThreshold) {
      double r = rand.nextDouble();
      if(r < compactionProbability) {
        numCellsInVersionedList = versionedList.getNumOfCells();
        compacted = true;
        return compact(versionedList, name+" (compaction probability="+compactionProbability+")");
      }
    }
    compacted = false;
    return simpleMergeOrFlatten(versionedList,
        name+" (compaction probability="+compactionProbability+")");
  }

  @Override
  public void updateStats(Segment replacement) {
    if(compacted) {
      if (replacement.getCellsCount() / numCellsInVersionedList < 1.0 - compactionThreshold) {
        // compaction was a good decision - increase probability
        compactionProbability *= MAGIC_PROBABILITY_FACTOR;
        if(compactionProbability > 1.0) {
          compactionProbability = 1.0;
        }
      } else {
        // compaction was NOT a good decision - decrease probability
        compactionProbability /= MAGIC_PROBABILITY_FACTOR;
      }
    }
  }

  @Override
  public void resetStats() {
    compactionProbability = initialCompactionProbability;
  }

  protected Action getMergingAction() {
    return Action.MERGE_COUNT_UNIQUES;
  }

  protected Action getFlattenAction() {
    return Action.FLATTEN;
  }

}
