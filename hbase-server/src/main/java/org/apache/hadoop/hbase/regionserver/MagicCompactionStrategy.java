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

public class MagicCompactionStrategy extends MemStoreCompactionStrategy{

  private static final String name = "MAGIC";
  public static final String MAGIC_COMPACTION_THRESHOLD_KEY =
      "hbase.hregion.magic.compaction.threshold";
  private static final double MAGIC_COMPACTION_THRESHOLD_DEFAULT = 0.3;

  private double compactionThreshold;

  public MagicCompactionStrategy(Configuration conf, String cfName) {
    super(conf, cfName);
    compactionThreshold = conf.getDouble(MAGIC_COMPACTION_THRESHOLD_KEY,
        MAGIC_COMPACTION_THRESHOLD_DEFAULT);
  }

  @Override public Action getAction(VersionedSegmentsList versionedList) {
    if (versionedList.getAvgUniquesFrac() < 1.0 - compactionThreshold) {
        return compact(versionedList, name);
      }
    return simpleMergeOrFlatten(versionedList, name);
  }

  protected Action getMergingAction() {
    return Action.MERGE_COUNT_UNIQUES;
  }

  protected Action getFlattenAction() {
    return Action.FLATTEN_COUNT_UNIQUES;
  }

}
