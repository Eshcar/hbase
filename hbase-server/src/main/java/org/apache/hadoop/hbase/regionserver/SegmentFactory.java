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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A singleton store segment factory.
 * Generate concrete store segments.
 */
@InterfaceAudience.Private
public final class SegmentFactory {

  private SegmentFactory() {}
  private static SegmentFactory instance = new SegmentFactory();

  public static SegmentFactory instance() {
    return instance;
  }

  // create skip-list-based (non-flat) immutable segment from compacting old immutable segments
  // --- currently not in use ---
  public ImmutableSegment createImmutableSegment(final Configuration conf,
      final CellComparator comparator, MemStoreSegmentsIterator iterator) {
    return new ImmutableCSLMSegment(comparator, iterator, MemStoreLAB.newInstance(conf));
  }

  // create composite immutable segment from a list of segments
  // for snapshot consisting of multiple segments
  public CompositeImmutableSegment createCompositeImmutableSegment(
      final CellComparator comparator, List<ImmutableSegment> segments) {
    return new CompositeImmutableSegment(comparator, segments);
  }

  // create new flat immutable segment from compacting old immutable segments
  // for compaction
  public ImmutableSegment createImmutableSegmentByCompaction(final Configuration conf,
      final CellComparator comparator, MemStoreSegmentsIterator iterator, int numOfCells)
      throws IOException {

    MemStoreLAB memStoreLAB = MemStoreLAB.newInstance(conf);
    boolean toCellChunkMap =  conf.getBoolean(CompactingMemStore.COMPACTING_MEMSTORE_CHUNK_MAP_KEY,
        CompactingMemStore.COMPACTING_MEMSTORE_CHUNK_MAP_DEFAULT);

    // the last parameter "false" means not to merge, but to compact the pipeline
    // in order to create the new segment
    return toCellChunkMap ?
        new ImmutableCellChunkSegment(comparator, iterator, memStoreLAB, numOfCells, false):
        new ImmutableCellArraySegment(comparator, iterator, memStoreLAB, numOfCells, false);
  }

  // create empty immutable segment
  // for initializations
  public ImmutableSegment createImmutableSegment(CellComparator comparator) {
    MutableSegment segment = generateMutableSegment(null, comparator, null);
    return createImmutableSegment(segment);
  }

  // create not-flat immutable segment from mutable segment
  // for initial push into the pipeline
  public ImmutableSegment createImmutableSegment(MutableSegment segment) {
    return new ImmutableCSLMSegment(segment);
  }

  // create mutable segment
  public MutableSegment createMutableSegment(final Configuration conf, CellComparator comparator) {
    MemStoreLAB memStoreLAB = MemStoreLAB.newInstance(conf);
    return generateMutableSegment(conf, comparator, memStoreLAB);
  }

  // create new flat immutable segment from merging old immutable segments
  // for merge
  public ImmutableSegment createImmutableSegmentByMerge(final Configuration conf,
      final CellComparator comparator, MemStoreSegmentsIterator iterator, int numOfCells,
      List<ImmutableSegment> segments)
      throws IOException {

    MemStoreLAB memStoreLAB = getMergedMemStoreLAB(conf, segments);
    boolean toCellChunkMap =  conf.getBoolean(CompactingMemStore.COMPACTING_MEMSTORE_CHUNK_MAP_KEY,
        CompactingMemStore.COMPACTING_MEMSTORE_CHUNK_MAP_DEFAULT);
    // the last parameter "true" means to merge the compaction pipeline
    // in order to create the new segment
    return toCellChunkMap ?
        new ImmutableCellChunkSegment(comparator, iterator, memStoreLAB, numOfCells, true):
        new ImmutableCellArraySegment(comparator, iterator, memStoreLAB, numOfCells, true);

  }

  // create flat immutable segment from non-flat immutable segment
  // for flattening
  public ImmutableSegment createImmutableSegmentByFlattening(
      ImmutableCSLMSegment segment, boolean toCellChunkMap, MemstoreSize memstoreSize) {
    return toCellChunkMap ?
        new ImmutableCellChunkSegment(segment, memstoreSize):
        new ImmutableCellArraySegment(segment, memstoreSize);
  }
  //****** private methods to instantiate concrete store segments **********//
  private MutableSegment generateMutableSegment(final Configuration conf, CellComparator comparator,
      MemStoreLAB memStoreLAB) {
    // TBD use configuration to set type of segment
    CellSet set = new CellSet(comparator);
    return new MutableSegment(set, comparator, memStoreLAB);
  }

  private MemStoreLAB getMergedMemStoreLAB(Configuration conf, List<ImmutableSegment> segments) {
    List<MemStoreLAB> mslabs = new ArrayList<>();
    if (!conf.getBoolean(MemStoreLAB.USEMSLAB_KEY, MemStoreLAB.USEMSLAB_DEFAULT)) {
      return null;
    }
    for (ImmutableSegment segment : segments) {
      mslabs.add(segment.getMemStoreLAB());
    }
    return new ImmutableMemStoreLAB(mslabs);
  }
}
