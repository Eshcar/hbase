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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import java.util.NavigableSet;

/**
 * This is an abstraction of a cell set bucket maintained in a memstore, e.g., the active
 * cell set or a snapshot of it.
 * It mainly encapsulates the kv-set and its respective memory allocation buffers (MSLAB).
 * This class facilitates the management of the compaction pipeline and the shifts of these buckets
 * from active set to snapshot set in the default implementation.
 */
@InterfaceAudience.Private
class CellSetMgr {

  private volatile NavigableSet<Cell> cellSet;
  private volatile MemStoreLAB memStoreLAB;

  // private c-tors. Instantiate objects only using factory
  private CellSetMgr(NavigableSet<Cell> cellSet, MemStoreLAB memStoreLAB) {
    this.cellSet = cellSet;
    this.memStoreLAB = memStoreLAB;
  }
  private CellSetMgr(NavigableSet<Cell> cellSet) {
    this(cellSet,null);
  }

  KeyValueScanner getScanner(long readPoint) {
    return new CellSetScanner(this, readPoint);
  }

  /**
   * Types of cell set managers.
   * This affects the internal implementation of the cell set objects.
   * This allows using different formats for different purposes.
   */
  static public enum CellSetMgrType {
    READ_WRITE,
    COMPACTED_READ_ONLY
  }

  /**
   * A singleton cell set manager factory.
   * Maps each cell set type to a specific implementation
   */
  static class CellSetMgrFactory {

    private CellSetMgrFactory() {}
    private static CellSetMgrFactory instance = new CellSetMgrFactory();
    public static CellSetMgrFactory instance() { return instance; }

    public CellSetMgr createCellSetMgr(CellSetMgrType type, final Configuration conf,
        final KeyValue.KVComparator comparator) {
      MemStoreLAB memStoreLAB = null;
      if (conf.getBoolean(DefaultMemStore.USEMSLAB_KEY, DefaultMemStore.USEMSLAB_DEFAULT)) {
        String className = conf.get(DefaultMemStore.MSLAB_CLASS_NAME,
            HeapMemStoreLAB.class.getName());
        memStoreLAB = ReflectionUtils.instantiateWithCustomCtor(className,
            new Class[] { Configuration.class }, new Object[] { conf });
      }
      return createCellSetMgr(type,comparator,memStoreLAB);
    }

    public CellSetMgr createCellSetMgr(CellSetMgrType type, KeyValue.KVComparator comparator) {
      return createCellSetMgr(type,comparator,null);
    }

    public CellSetMgr createCellSetMgr(CellSetMgrType type, KeyValue.KVComparator comparator,
        MemStoreLAB memStoreLAB) {
      return generateCellSetMgrByType(type,comparator,memStoreLAB);
    }

    private CellSetMgr generateCellSetMgrByType(CellSetMgrType type,
        KeyValue.KVComparator comparator, MemStoreLAB memStoreLAB) {
      CellSetMgr obj;
      NavigableSet<Cell> cellNavigableSet = new CellSkipListSet(comparator);
      switch (type) {
      case READ_WRITE:
      case COMPACTED_READ_ONLY:
      default:
        obj = new CellSetMgr(cellNavigableSet, memStoreLAB);
      }
      return obj;
    }

  }
}
