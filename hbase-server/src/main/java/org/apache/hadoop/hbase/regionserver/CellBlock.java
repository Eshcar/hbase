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

import org.apache.hadoop.hbase.Cell;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;



/**
 * CellBlock stores a constant number of elements and is immutable after creation stage.
 * Due to being immutable the CellBlock can be implemented as array.
 * The actual array is on- or off-heap and is implemented in concrete class derived from CellBlock.
 * The CellBlock uses no synchronization primitives, it is assumed to be created by a
 * single thread and then it can be read-only by multiple threads.
 */
public abstract class CellBlock implements ConcurrentNavigableMap<Cell,Cell> {

  private final Comparator<? super Cell> comparator;
  private int minCellIdx   = 0;   // the index of the minimal cell (for sub-sets)
  private int maxCellIdx   = 0;   // the index of the maximal cell (for sub-sets)
  private boolean descending = false;

  /* C-tor */
  public CellBlock(Comparator<? super Cell> comparator, int min, int max, boolean d){
    this.comparator = comparator;
    this.minCellIdx = min;
    this.maxCellIdx = max;
    this.descending = d;
  }

  /* Used for abstract CellBlock creation, implemented by derived class */
  protected abstract CellBlock createCellBlocks(Comparator<? super Cell> comparator, int min,
      int max, boolean descending);

  /* Returns the i-th cell in the cell block */
  protected abstract Cell getCellFromIndex(int i);

  /**
   * Binary search for a given key in between given boundaries of the array.
   * Positive returned numbers mean the index.
   * Negative returned numbers means the key not found.
   * The absolute value of the output is the
   * possible insert index for the searched key: (-1 * insertion point) - 1
   * @param needle The key to look for in all of the entries
   * @return Same return value as Arrays.binarySearch.
   */
  private int find(Cell needle) {
    int begin = minCellIdx;
    int end = maxCellIdx - 1;

    while (begin <= end) {
      int mid = begin + ((end - begin) / 2);

      Cell midCell = getCellFromIndex(mid);

      int compareRes = comparator.compare(midCell, needle);

      // 0 means equals. We found the key.
      if (compareRes == 0) return mid;
      else if (compareRes < 0) {
        // midCell is less than needle so we need to look at farther up
        begin = mid + 1;
      } else {
        // midCell is greater than needle so we need to look down
        end = mid - 1;
      }
    }

    return (-1 * begin) - 1;
  }

  private int getValidIndex(Cell key, boolean inclusive) {
    int index = find(key);
    if (inclusive && index >= 0) index++;
    else if (index < 0) index = -(index + 1) - 1;
    return index;
  }

  @Override
  public Comparator<? super Cell> comparator() {
    return comparator;
  }

  @Override
  public int size() {
    return maxCellIdx-minCellIdx;
  }

  @Override
  public boolean isEmpty() {
    return (maxCellIdx==minCellIdx);
  }


  // ---------------- Sub-Maps ----------------
  @Override
  public ConcurrentNavigableMap<Cell, Cell> subMap( Cell fromKey,
                                                    boolean fromInclusive,
                                                    Cell toKey,
                                                    boolean toInclusive) {
    int toIndex = getValidIndex(toKey, toInclusive);
    int fromIndex = (getValidIndex(fromKey, !fromInclusive));

    if (fromIndex > toIndex) throw new IllegalArgumentException("inconsistent range");
    return createCellBlocks(comparator, fromIndex, toIndex, descending);
  }

  @Override
  public ConcurrentNavigableMap<Cell, Cell> headMap(Cell toKey, boolean inclusive) {
    int index = getValidIndex(toKey, inclusive);
    return createCellBlocks(comparator, minCellIdx, index, descending);
  }

  @Override
  public ConcurrentNavigableMap<Cell, Cell> tailMap(Cell fromKey, boolean inclusive) {
    int index = (getValidIndex(fromKey, !inclusive));
    return createCellBlocks(comparator, index, maxCellIdx, descending);
  }

  @Override
  public ConcurrentNavigableMap<Cell, Cell> descendingMap() {
    return createCellBlocks(comparator, minCellIdx, maxCellIdx, true);
  }

  @Override
  public ConcurrentNavigableMap<Cell, Cell> subMap(Cell k1, Cell k2) {
    return this.subMap(k1, true, k2, true);
  }

  @Override 
  public ConcurrentNavigableMap<Cell, Cell> headMap(Cell k) {
    return this.headMap(k, true);
  }

  @Override 
  public ConcurrentNavigableMap<Cell, Cell> tailMap(Cell k) {
    return this.tailMap(k, true);
  }


  // -------------------------------- Key's getters --------------------------------
  @Override
  public Cell firstKey() {
    if (isEmpty()) return null;
    if (descending) getCellFromIndex(maxCellIdx-1);
    return getCellFromIndex(minCellIdx);
  }

  @Override
  public Cell lastKey() {
    if (isEmpty()) return null;
    if (descending) return getCellFromIndex(minCellIdx);
    return getCellFromIndex(maxCellIdx-1);
  }

  @Override
  public Cell lowerKey(Cell k) {
    if (isEmpty()) return null;
    int index = find(k);
    if (descending) {
      if (index >= 0) index++; // There's a key exactly equal.
      else index = -(index + 1);
    } else {
      if (index >= 0) index--; // There's a key exactly equal.
      else index = -(index + 1) - 1;
    }
    return (index < minCellIdx || index >= maxCellIdx) ? null : getCellFromIndex(index);
  }

  @Override
  public Cell floorKey(Cell k) {
    if (isEmpty()) return null;
    int index = find(k);
    if (descending) {
      if (index < 0)  index = -(index + 1);
    } else {
      if (index < 0) index = -(index + 1) - 1;
    }
    return (index < minCellIdx || index >= maxCellIdx) ? null : getCellFromIndex(index);
  }

  @Override
  public Cell ceilingKey(Cell k) {
    if (isEmpty()) return null;
    int index = find(k);
    if (descending) {
      if (index < 0) index = -(index + 1) - 1;
    } else {
      if (index < 0) index = -(index + 1);
    }
    return (index < minCellIdx || index >= maxCellIdx) ? null : getCellFromIndex(index);
  }

  @Override
  public Cell higherKey(Cell k) {
    if (isEmpty()) return null;
    int index = find(k);
    if (descending) {
      if (index >= 0) index--; // There's a key exactly equal.
      else index = -(index + 1) - 1;
    } else {
      if (index >= 0) index++; // There's a key exactly equal.
      else index = -(index + 1);
    }
    return (index < minCellIdx || index >= maxCellIdx) ? null : getCellFromIndex(index);
  }

  @Override
  public boolean containsKey(Object o) {
    int index = find((Cell) o);
    return (index >= 0);
  }

  @Override
  public boolean containsValue(Object o) { // use containsKey(Object o) instead
    throw new UnsupportedOperationException();
  }

  @Override
  public Cell get(Object o) {
    int index = find((Cell) o);
    if (index >= 0) {
      return getCellFromIndex(index);
    }
    return null;
  }

  // -------------------------------- Entry's getters --------------------------------
  // all interfaces returning Entries are unsupported because we are dealing only with the keys
  @Override 
  public Entry<Cell, Cell> lowerEntry(Cell k) {
    throw new UnsupportedOperationException();
  }

  @Override 
  public Entry<Cell, Cell> higherEntry(Cell k) {
    throw new UnsupportedOperationException();
  }

  @Override 
  public Entry<Cell, Cell> ceilingEntry(Cell k) {
    throw new UnsupportedOperationException();
  }

  @Override 
  public Entry<Cell, Cell> floorEntry(Cell k) {
    throw new UnsupportedOperationException();
  }

  @Override 
  public Entry<Cell, Cell> firstEntry() {
    throw new UnsupportedOperationException();
  }

  @Override 
  public Entry<Cell, Cell> lastEntry() {
    throw new UnsupportedOperationException();
  }

  @Override 
  public Entry<Cell, Cell> pollFirstEntry() {
    throw new UnsupportedOperationException();
  }

  @Override 
  public Entry<Cell, Cell> pollLastEntry() {
    throw new UnsupportedOperationException();
  }


  // -------------------------------- Updates --------------------------------
  // All updating methods below are unsupported.
  // Assuming an array of Cells will be allocated externally,
  // fill up with Cells and provided in construction time.
  // Later the structure is immutable.
  @Override
  public Cell put(Cell k, Cell v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Cell remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override 
  public boolean replace(Cell k, Cell v, Cell v1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends Cell, ? extends Cell> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Cell putIfAbsent(Cell k, Cell v) {
    throw new UnsupportedOperationException();
  }

  @Override 
  public boolean remove(Object o, Object o1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Cell replace(Cell k, Cell v) {
    throw new UnsupportedOperationException();
  }


  // -------------------------------- Sub-Sets --------------------------------
  @Override
  public NavigableSet<Cell> navigableKeySet() {
    throw new UnsupportedOperationException();
  }

  @Override 
  public NavigableSet<Cell> descendingKeySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public NavigableSet<Cell> keySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Cell> values() {
    return new CellBlocksCollection();
  }

  @Override
  public Set<Entry<Cell, Cell>> entrySet() {
    throw new UnsupportedOperationException();
  }


  // -------------------------------- Iterator K --------------------------------
  private final class CellBlocksIterator implements Iterator<Cell> {
    int index;

    private CellBlocksIterator() {
      index = descending ? maxCellIdx-1 : minCellIdx;
    }

    @Override
    public boolean hasNext() {
      return descending ? (index >= minCellIdx) : (index < maxCellIdx);
    }

    @Override
    public Cell next() {
      Cell result = getCellFromIndex(index);
      if (descending) index--;
      else index++;
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  
  // -------------------------------- Collection --------------------------------
  private final class CellBlocksCollection implements Collection<Cell> {

    @Override
    public int size()         {
      return CellBlock.this.size();
    }

    @Override
    public boolean isEmpty()  {
      return CellBlock.this.isEmpty();
    }

    @Override
    public void clear()       {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
      return containsKey(o);
    }

    @Override
    public Iterator<Cell> iterator() {
      return new CellBlocksIterator();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Cell k) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Cell> collection) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }


  }

}
