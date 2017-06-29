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
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public abstract class MemStoreCompactionStrategy {

  protected static final Log LOG = LogFactory.getLog(MemStoreCompactionStrategy.class);
  // The upper bound for the number of segments we store in the pipeline prior to merging.
  public static final String COMPACTING_MEMSTORE_THRESHOLD_KEY =
      "hbase.hregion.compacting.pipeline.segments.limit";
  public static final int COMPACTING_MEMSTORE_THRESHOLD_DEFAULT = 1;

  public void resetDuplicationInfo() {}

  public void updateDuplicationInfo(VersionedSegmentsList versionedList, ImmutableSegment result) {
  }

  public void updateDuplicationInfo(CellSet cellSet) {
  }

  /**
   * Types of actions to be done on the pipeline upon MemStoreCompaction invocation.
   * Note that every value covers the previous ones, i.e. if MERGE is the action it implies
   * that the youngest segment is going to be flatten anyway.
   */
  public enum Action {
    NOOP,
    FLATTEN,  // flatten the youngest segment in the pipeline
    MERGE,    // merge all the segments in the pipeline into one
    COMPACT   // copy-compact the data of all the segments in the pipeline
  }

  protected final String cfName;
  // The limit on the number of the segments in the pipeline
  protected final int pipelineThreshold;


  public MemStoreCompactionStrategy(Configuration conf, String cfName) {
    this.cfName = cfName;
    if(conf == null) {
      pipelineThreshold = COMPACTING_MEMSTORE_THRESHOLD_DEFAULT;
    } else {
      pipelineThreshold =         // get the limit on the number of the segments in the pipeline
          conf.getInt(COMPACTING_MEMSTORE_THRESHOLD_KEY, COMPACTING_MEMSTORE_THRESHOLD_DEFAULT);
    }
  }

  // get next compaction action to apply on compaction pipeline
  public abstract Action getAction(VersionedSegmentsList versionedList);

  protected Action simpleMergeOrFlatten(VersionedSegmentsList versionedList, String strategy) {
    int numOfSegments = versionedList.getNumOfSegments();
    if (numOfSegments > pipelineThreshold) {
      // to avoid too many segments, merge now
      LOG.debug(strategy+" memory compaction for store " + cfName
          + " merging " + numOfSegments + " segments");
      return Action.MERGE;
    }

    // just flatten the newly joined segment
    LOG.debug(strategy+" memory compaction for store " + cfName
        + "flattening the youngest segment in the pipeline");
    return Action.FLATTEN;
  }

}
