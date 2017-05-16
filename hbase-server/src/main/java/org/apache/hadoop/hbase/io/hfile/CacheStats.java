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
package org.apache.hadoop.hbase.io.hfile;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.impl.FastLongHistogram;


/**
 * Class that implements cache metrics.
 */
@InterfaceAudience.Private
public class CacheStats {
  private static final Log LOG = LogFactory.getLog(CacheStats.class.getName());

  /** Sliding window statistics. The number of metric periods to include in
   * sliding window hit ratio calculations.
   */
  static final int DEFAULT_WINDOW_PERIODS = 5;

  /** The number of getBlock requests that were cache hits */
  private final LongAdder hitCount = new LongAdder();

  /** The number of getBlock requests that were cache hits from primary replica */
  private final LongAdder primaryHitCount = new LongAdder();

  /**
   * The number of getBlock requests that were cache hits, but only from
   * requests that were set to use the block cache.  This is because all reads
   * attempt to read from the block cache even if they will not put new blocks
   * into the block cache.  See HBASE-2253 for more information.
   */
  private final LongAdder hitCachingCount = new LongAdder();

  /** The number of getBlock requests that were cache misses */
  private final LongAdder missCount = new LongAdder();

  /** The number of getBlock requests for primary replica that were cache misses */
  private final LongAdder primaryMissCount = new LongAdder();
  /**
   * The number of getBlock requests that were cache misses, but only from
   * requests that were set to use the block cache.
   */
  private final LongAdder missCachingCount = new LongAdder();

  /** The number of times an eviction has occurred */
  private final LongAdder evictionCount = new LongAdder();

  /** The total number of blocks that have been evicted */
  private final LongAdder evictedBlockCount = new LongAdder();

  /** The total number of blocks for primary replica that have been evicted */
  private final LongAdder primaryEvictedBlockCount = new LongAdder();

  /** The total number of blocks that were not inserted. */
  private final AtomicLong failedInserts = new AtomicLong(0);

  /** Per Block Type Counts */
  private final LongAdder dataMissCount = new LongAdder();
  private final LongAdder leafIndexMissCount = new LongAdder();
  private final LongAdder bloomChunkMissCount = new LongAdder();
  private final LongAdder metaMissCount = new LongAdder();
  private final LongAdder rootIndexMissCount = new LongAdder();
  private final LongAdder intermediateIndexMissCount = new LongAdder();
  private final LongAdder fileInfoMissCount = new LongAdder();
  private final LongAdder generalBloomMetaMissCount = new LongAdder();
  private final LongAdder deleteFamilyBloomMissCount = new LongAdder();
  private final LongAdder trailerMissCount = new LongAdder();

  private final LongAdder dataHitCount = new LongAdder();
  private final LongAdder leafIndexHitCount = new LongAdder();
  private final LongAdder bloomChunkHitCount = new LongAdder();
  private final LongAdder metaHitCount = new LongAdder();
  private final LongAdder rootIndexHitCount = new LongAdder();
  private final LongAdder intermediateIndexHitCount = new LongAdder();
  private final LongAdder fileInfoHitCount = new LongAdder();
  private final LongAdder generalBloomMetaHitCount = new LongAdder();
  private final LongAdder deleteFamilyBloomHitCount = new LongAdder();
  private final LongAdder trailerHitCount = new LongAdder();

  /** The number of metrics periods to include in window */
  private final int numPeriodsInWindow;
  /** Hit counts for each period in window */
  private final long [] hitCounts;
  /** Caching hit counts for each period in window */
  private final long [] hitCachingCounts;
  /** Access counts for each period in window */
  private final long [] requestCounts;
  /** Caching access counts for each period in window */
  private final long [] requestCachingCounts;
  /** Last hit count read */
  private long lastHitCount = 0;
  /** Last hit caching count read */
  private long lastHitCachingCount = 0;
  /** Last request count read */
  private long lastRequestCount = 0;
  /** Last request caching count read */
  private long lastRequestCachingCount = 0;
  /** Current window index (next to be updated) */
  private int windowIndex = 0;
  /**
   * Keep running age at eviction time
   */
  private FastLongHistogram ageAtEviction;
  private long startTime = System.nanoTime();

  public CacheStats(final String name) {
    this(name, DEFAULT_WINDOW_PERIODS);
  }

  public CacheStats(final String name, int numPeriodsInWindow) {
    this.numPeriodsInWindow = numPeriodsInWindow;
    this.hitCounts = new long[numPeriodsInWindow];
    this.hitCachingCounts =  new long[numPeriodsInWindow];
    this.requestCounts =  new long[numPeriodsInWindow];
    this.requestCachingCounts =  new long[numPeriodsInWindow];
    this.ageAtEviction = new FastLongHistogram();
  }

  @Override
  public String toString() {
    AgeSnapshot snapshot = getAgeAtEvictionSnapshot();
    return "hitCount=" + getHitCount() + ", hitCachingCount=" + getHitCachingCount() +
      ", missCount=" + getMissCount() + ", missCachingCount=" + getMissCachingCount() +
      ", evictionCount=" + getEvictionCount() +
      ", evictedBlockCount=" + getEvictedCount() +
      ", primaryMissCount=" + getPrimaryMissCount() +
      ", primaryHitCount=" + getPrimaryHitCount() +
      ", evictedAgeMean=" + snapshot.getMean();
  }


  public void miss(boolean caching, boolean primary, BlockType type) {
    missCount.increment();
    if (primary) primaryMissCount.increment();
    if (caching) missCachingCount.increment();
    if (type == null) {
      return;
    }
    switch (type) {
      case DATA:
      case ENCODED_DATA:
        dataMissCount.increment();
        LOG.info("CacheStats "+" dataMissCount"+ dataMissCount.longValue());
        break;
      case LEAF_INDEX:
        leafIndexMissCount.increment();
        LOG.info("CacheStats " + " leafIndexMissCount" + leafIndexMissCount.longValue());
        break;
      case BLOOM_CHUNK:
        bloomChunkMissCount.increment();
        LOG.info("CacheStats " + " bloomChunkMissCount" + bloomChunkMissCount.longValue());
        break;
      case META:
        metaMissCount.increment();
        LOG.info("CacheStats " + " metaMissCount" + metaMissCount.longValue());
        break;
      case INTERMEDIATE_INDEX:
        intermediateIndexMissCount.increment();
        LOG.info("CacheStats " + " intermediateIndexMissCount" + intermediateIndexMissCount.longValue());
        break;
      case ROOT_INDEX:
        rootIndexMissCount.increment();
        LOG.info("CacheStats " + " rootIndexMissCount" + rootIndexMissCount.longValue());
        break;
      case FILE_INFO:
        fileInfoMissCount.increment();
        LOG.info("CacheStats " + " fileInfoMissCount" + fileInfoMissCount.longValue());
        break;
      case GENERAL_BLOOM_META:
        generalBloomMetaMissCount.increment();
        LOG.info("CacheStats " + " generalBloomMetaMissCount" + generalBloomMetaMissCount.longValue());
        break;
      case DELETE_FAMILY_BLOOM_META:
        deleteFamilyBloomMissCount.increment();
        LOG.info("CacheStats " + " deleteFamilyBloomMissCount" + deleteFamilyBloomMissCount.longValue());
        break;
    case TRAILER:
        trailerMissCount.increment();
        LOG.info("CacheStats "+" trailerMissCount"+ trailerMissCount.longValue());
        break;
      default:
        // If there's a new type that's fine
        // Ignore it for now. This is metrics don't exception.
        break;
    }
  }

  public void hit(boolean caching, boolean primary, BlockType type) {
    hitCount.increment();
    if (primary) primaryHitCount.increment();
    if (caching) hitCachingCount.increment();


    if (type == null) {
      return;
    }
    switch (type) {
      case DATA:
      case ENCODED_DATA:
        dataHitCount.increment();
        LOG.info("CacheStats " + " dataHitCount" + dataHitCount.longValue());
        break;
      case LEAF_INDEX:
        leafIndexHitCount.increment();
        LOG.info("CacheStats " + " leafIndexHitCount" + leafIndexHitCount.longValue());
        break;
      case BLOOM_CHUNK:
        bloomChunkHitCount.increment();
        LOG.info("CacheStats " + " bloomChunkHitCount" + bloomChunkHitCount.longValue());
        break;
      case META:
        metaHitCount.increment();
        LOG.info("CacheStats " + " metaHitCount" + metaHitCount.longValue());
        break;
      case INTERMEDIATE_INDEX:
        intermediateIndexHitCount.increment();
        LOG.info("CacheStats " + " intermediateIndexHitCount" + intermediateIndexHitCount.longValue());
        break;
      case ROOT_INDEX:
        rootIndexHitCount.increment();
        LOG.info("CacheStats " + " rootIndexHitCount" + rootIndexHitCount.longValue());
        break;
      case FILE_INFO:
        fileInfoHitCount.increment();
        LOG.info("CacheStats " + " fileInfoHitCount" + fileInfoHitCount.longValue());
        break;
      case GENERAL_BLOOM_META:
        generalBloomMetaHitCount.increment();
        LOG.info("CacheStats " + " generalBloomMetaHitCount" + generalBloomMetaHitCount.longValue());
        break;
      case DELETE_FAMILY_BLOOM_META:
        deleteFamilyBloomHitCount.increment();
        LOG.info("CacheStats " + " deleteFamilyBloomHitCount" + deleteFamilyBloomHitCount.longValue());
        break;
      case TRAILER:
        trailerHitCount.increment();
        LOG.info("CacheStats " + " trailerHitCount" + trailerHitCount.longValue());
        break;
      default:
        // If there's a new type that's fine
        // Ignore it for now. This is metrics don't exception.
        break;
    }
  }

  public void evict() {
    evictionCount.increment();
  }

  public void evicted(final long t, boolean primary) {
    if (t > this.startTime) {
      this.ageAtEviction.add((t - this.startTime) / BlockCacheUtil.NANOS_PER_SECOND, 1);
    }
    this.evictedBlockCount.increment();
    if (primary) {
      primaryEvictedBlockCount.increment();
    }
  }

  public long failInsert() {
    return failedInserts.incrementAndGet();
  }


  // All of the counts of misses and hits.
  public long getDataMissCount() {
    return dataMissCount.sum();
  }

  public long getLeafIndexMissCount() {
    return leafIndexMissCount.sum();
  }

  public long getBloomChunkMissCount() {
    return bloomChunkMissCount.sum();
  }

  public long getMetaMissCount() {
    return metaMissCount.sum();
  }

  public long getRootIndexMissCount() {
    return rootIndexMissCount.sum();
  }

  public long getIntermediateIndexMissCount() {
    return intermediateIndexMissCount.sum();
  }

  public long getFileInfoMissCount() {
    return fileInfoMissCount.sum();
  }

  public long getGeneralBloomMetaMissCount() {
    return generalBloomMetaMissCount.sum();
  }

  public long getDeleteFamilyBloomMissCount() {
    return deleteFamilyBloomMissCount.sum();
  }

  public long getTrailerMissCount() {
    return trailerMissCount.sum();
  }

  public long getDataHitCount() {
    return dataHitCount.sum();
  }

  public long getLeafIndexHitCount() {
    return leafIndexHitCount.sum();
  }

  public long getBloomChunkHitCount() {
    return bloomChunkHitCount.sum();
  }

  public long getMetaHitCount() {
    return metaHitCount.sum();
  }

  public long getRootIndexHitCount() {
    return rootIndexHitCount.sum();
  }

  public long getIntermediateIndexHitCount() {
    return intermediateIndexHitCount.sum();
  }

  public long getFileInfoHitCount() {
    return fileInfoHitCount.sum();
  }

  public long getGeneralBloomMetaHitCount() {
    return generalBloomMetaHitCount.sum();
  }

  public long getDeleteFamilyBloomHitCount() {
    return deleteFamilyBloomHitCount.sum();
  }

  public long getTrailerHitCount() {
    return trailerHitCount.sum();
  }

  public long getRequestCount() {
    return getHitCount() + getMissCount();
  }

  public long getRequestCachingCount() {
    return getHitCachingCount() + getMissCachingCount();
  }

  public long getMissCount() {
    return missCount.sum();
  }

  public long getPrimaryMissCount() {
    return primaryMissCount.sum();
  }

  public long getMissCachingCount() {
    return missCachingCount.sum();
  }

  public long getHitCount() {
    return hitCount.sum();
  }

  public long getPrimaryHitCount() {
    return primaryHitCount.sum();
  }

  public long getHitCachingCount() {
    return hitCachingCount.sum();
  }

  public long getEvictionCount() {
    return evictionCount.sum();
  }

  public long getEvictedCount() {
    return this.evictedBlockCount.sum();
  }

  public long getPrimaryEvictedCount() {
    return primaryEvictedBlockCount.sum();
  }

  public double getHitRatio() {
    return ((double) getHitCount() / (double) getRequestCount());
  }

  public double getHitCachingRatio() {
    return ((double) getHitCachingCount() / (double) getRequestCachingCount());
  }

  public double getMissRatio() {
    return ((double) getMissCount() / (double) getRequestCount());
  }

  public double getMissCachingRatio() {
    return ((double) getMissCachingCount() / (double) getRequestCachingCount());
  }

  public double evictedPerEviction() {
    return ((double) getEvictedCount() / (double) getEvictionCount());
  }

  public long getFailedInserts() {
    return failedInserts.get();
  }

  public void rollMetricsPeriod() {
    hitCounts[windowIndex] = getHitCount() - lastHitCount;
    lastHitCount = getHitCount();
    hitCachingCounts[windowIndex] =
      getHitCachingCount() - lastHitCachingCount;
    lastHitCachingCount = getHitCachingCount();
    requestCounts[windowIndex] = getRequestCount() - lastRequestCount;
    lastRequestCount = getRequestCount();
    requestCachingCounts[windowIndex] =
      getRequestCachingCount() - lastRequestCachingCount;
    lastRequestCachingCount = getRequestCachingCount();
    windowIndex = (windowIndex + 1) % numPeriodsInWindow;
  }

  public long getSumHitCountsPastNPeriods() {
    return sum(hitCounts);
  }

  public long getSumRequestCountsPastNPeriods() {
    return sum(requestCounts);
  }

  public long getSumHitCachingCountsPastNPeriods() {
    return sum(hitCachingCounts);
  }

  public long getSumRequestCachingCountsPastNPeriods() {
    return sum(requestCachingCounts);
  }

  public double getHitRatioPastNPeriods() {
    double ratio = ((double)getSumHitCountsPastNPeriods() /
        (double)getSumRequestCountsPastNPeriods());
    return Double.isNaN(ratio) ? 0 : ratio;
  }

  public double getHitCachingRatioPastNPeriods() {
    double ratio = ((double)getSumHitCachingCountsPastNPeriods() /
        (double)getSumRequestCachingCountsPastNPeriods());
    return Double.isNaN(ratio) ? 0 : ratio;
  }

  public AgeSnapshot getAgeAtEvictionSnapshot() {
    return new AgeSnapshot(this.ageAtEviction);
  }

  private static long sum(long[] counts) {
    return Arrays.stream(counts).sum();
  }
}
