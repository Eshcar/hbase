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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import java.util.SortedSet;

/**
 * This is an abstraction of a cell set bucket maintained in a memstore, e.g., the active
 * cell set or a snapshot of it.
 * It mainly encapsulates the kv-set and its respective memory allocation buffers (MSLAB).
 * This class facilitates the management of the compaction pipeline and the shifts of these buckets
 * from active set to snapshot set in the default implementation.
 */
@InterfaceAudience.Private
class CellSetMgr {

  static final String USEMSLAB_KEY = "hbase.hregion.memstore.mslab.enabled";
  static final boolean USEMSLAB_DEFAULT = true;
  static final String MSLAB_CLASS_NAME = "hbase.regionserver.mslab.class";

  private volatile CellSet cellSet;
  private volatile MemStoreLAB memStoreLAB;
  private TimeRangeTracker timeRangeTracker;

  // private c-tors. Instantiate objects only using factory
  private CellSetMgr(CellSet cellSet, MemStoreLAB memStoreLAB) {
    this.cellSet = cellSet;
    this.memStoreLAB = memStoreLAB;
    this.timeRangeTracker = new TimeRangeTracker();
  }
  private CellSetMgr(CellSet cellSet) {
    this(cellSet,null);
  }

  public CellSetScanner getScanner(long readPoint) {
    return new CellSetScanner(this, readPoint);
  }

  public CellSet getCellSet() {
    return cellSet;
  }

  public boolean isEmpty() {
    return getCellSet().isEmpty();
  }

  public int size() {
    return getCellSet().size();
  }

  public boolean add(Cell e) {
    return getCellSet().add(e);
  }

  public boolean remove(Cell e) {
    return getCellSet().remove(e);
  }

  public Cell get(Cell cell) {
    return getCellSet().get(cell);
  }

  public SortedSet<Cell> tailSet(Cell firstCell) {
    return getCellSet().tailSet(firstCell);
  }

  public void close() {
    getMemStoreLAB().close();
    // do not set MSLab to null as scanners may still be reading the data here and need to decrease
    // the counter when they finish
  }

  public Cell maybeCloneWithAllocator(Cell cell) {
    if (this.memStoreLAB == null) {
      return cell;
    }

    int len = KeyValueUtil.length(cell);
    ByteRange alloc = this.memStoreLAB.allocateBytes(len);
    if (alloc == null) {
      // The allocation was too large, allocator decided
      // not to do anything with it.
      return cell;
    }
    assert alloc.getBytes() != null;
    KeyValueUtil.appendToByteArray(cell, alloc.getBytes(), alloc.getOffset());
    KeyValue newKv = new KeyValue(alloc.getBytes(), alloc.getOffset(), len);
    newKv.setSequenceId(cell.getSequenceId());
    return newKv;
  }

  public SortedSet<Cell> headSet(Cell firstKeyOnRow) {
    return this.getCellSet().headSet(firstKeyOnRow);
  }

  public Cell last() {
    return this.getCellSet().last();
  }

  public void incScannerCount() {
    if(memStoreLAB != null) {
      memStoreLAB.incScannerCount();
    }
  }

  public void decScannerCount() {
    if(memStoreLAB != null) {
      memStoreLAB.decScannerCount();
    }
  }

  // methods for tests
  Cell first() {
    return this.getCellSet().first();
  }

  protected MemStoreLAB getMemStoreLAB() {
    return memStoreLAB;
  }

  public void includeTimestamp(Cell toAdd) {
    timeRangeTracker.includeTimestamp(toAdd);
  }

  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return (timeRangeTracker.includesTimeRange(scan.getTimeRange())
        && (timeRangeTracker.getMaximumTimestamp() >=
        oldestUnexpiredTS));
  }

  public TimeRangeTracker getTimeRangeTracker() {
    return timeRangeTracker;
  }

  /**
   * Types of cell set managers.
   * This affects the internal implementation of the cell set objects.
   * This allows using different formats for different purposes.
   */
  static public enum Type {
    READ_WRITE,
    EMPTY_SNAPSHOT,
    COMPACTED_READ_ONLY,
    DEFAULT
  }

  /**
   * A singleton cell set manager factory.
   * Maps each cell set type to a specific implementation
   */
  static class Factory {

    private Factory() {}
    private static Factory instance = new Factory();
    public static Factory instance() { return instance; }

    public CellSetMgr createCellSetMgr(Type type, final Configuration conf,
        final KeyValue.KVComparator comparator) {
      MemStoreLAB memStoreLAB = null;
      if (conf.getBoolean(USEMSLAB_KEY, USEMSLAB_DEFAULT)) {
        String className = conf.get(MSLAB_CLASS_NAME, HeapMemStoreLAB.class.getName());
        memStoreLAB = ReflectionUtils.instantiateWithCustomCtor(className,
            new Class[] { Configuration.class }, new Object[] { conf });
      }
      return createCellSetMgr(type,comparator,memStoreLAB);
    }

    public CellSetMgr createCellSetMgr(Type type, KeyValue.KVComparator comparator) {
      return createCellSetMgr(type,comparator,null);
    }

    public CellSetMgr createCellSetMgr(Type type, KeyValue.KVComparator comparator,
        MemStoreLAB memStoreLAB) {
      return generateCellSetMgrByType(type,comparator,memStoreLAB);
    }

    private CellSetMgr generateCellSetMgrByType(Type type,
        KeyValue.KVComparator comparator, MemStoreLAB memStoreLAB) {
      CellSetMgr obj;
      CellSet set = new CellSet(type, comparator);
      switch (type) {
      case READ_WRITE:
      case EMPTY_SNAPSHOT:
      case COMPACTED_READ_ONLY:
      default:
        obj = new CellSetMgr(set, memStoreLAB);
      }
      return obj;
    }

  }
}
