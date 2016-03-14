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

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * CellBlocks stores a constant number of elements and is immutable after creation stage.
 * Due to being immutable the CellBlocks can be implemented as array.
 * The actual array is on- or off-heap and is implemented in concrete class derived from CellBlocks.
 * The CellBlocks uses no synchronization primitives, it is assumed to be created by a
 * single thread and then it can be read-only by multiple threads.
 */
public abstract class CellBlocks implements ConcurrentNavigableMap<Cell,Cell> {

  private final Comparator<? super Cell> comparator;
  private int minCellIdx   = 0;   // the index of the minimal cell (for sub-sets)
  private int maxCellIdx   = 0;   // the index of the maximal cell (for sub-sets)
  private boolean isDescending = false; // array can be easily traversed backward in descending order

  /* C-tor */
  public CellBlocks(Comparator<? super Cell> comparator, int min, int max, boolean d){
    this.comparator = comparator;
    this.minCellIdx = min;
    this.maxCellIdx = max;
    this.isDescending = d;
  }

  /* Used for abstract CellBlocks creation, implemented by derived class */
  protected abstract CellBlocks createCellBlocks(Comparator<? super Cell> comparator, int min,
      int max, boolean d);

  /* Assuming array underneath implementation this comes instead of array[i] */
  protected abstract Cell getCellFromIndex(int i);

  /**
   * Binary search for a given key in between given boundaries of the array
   * @param needle The key to look for in all of the entries
   * @return Same return value as Arrays.binarySearch.
   * Positive numbers mean the index.
   * Otherwise (-1 * insertion point) - 1
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
        // midKey is less than needle so we need to look at farther up
        begin = mid + 1;
      } else {
        // midKey is greater than needle so we need to look down
        end = mid - 1;
      }
    }

    return (-1 * begin) - 1;
  }

  @Override public Comparator<? super Cell> comparator() { return comparator; }

  @Override public int size() { return maxCellIdx-minCellIdx; }

  @Override public boolean isEmpty() { return (maxCellIdx==minCellIdx); }


  // ---------------- Sub-Maps ----------------
  @Override public ConcurrentNavigableMap<Cell, Cell> subMap( Cell fromKey,
                                                        boolean fromInclusive,
                                                        Cell toKey,
                                                        boolean toInclusive) {
    int toIndex = find(toKey);
    if (toInclusive && toIndex >= 0) toIndex++;
    else if (toIndex < 0) toIndex = -(toIndex + 1) - 1;

    int fromIndex = find(fromKey);
    if (!fromInclusive && fromIndex >= 0) fromIndex++;
    else if (fromIndex < 0) fromIndex = -(fromIndex + 1);

    return createCellBlocks(comparator, fromIndex, toIndex, false);
  }

  @Override public ConcurrentNavigableMap<Cell, Cell> headMap(Cell toKey, boolean inclusive) {
    int index = find(toKey);
    if (inclusive && index >= 0) index++;
    else if (index < 0) index = -(index + 1) - 1;
    return createCellBlocks(comparator, minCellIdx, index, false);
  }

  @Override public ConcurrentNavigableMap<Cell, Cell> tailMap(Cell fromKey, boolean inclusive) {
    int index = find(fromKey);
    if (!inclusive && index >= 0) index++;
    else if (index < 0) index = -(index + 1);
    return createCellBlocks(comparator, index, maxCellIdx, false);
  }

  @Override public ConcurrentNavigableMap<Cell, Cell> descendingMap() {
    return createCellBlocks(comparator, minCellIdx, maxCellIdx, true);
  }

  @Override public ConcurrentNavigableMap<Cell, Cell> subMap(Cell k, Cell k1) {
    return this.subMap(k, true, k1, true);
  }

  @Override 
  public ConcurrentNavigableMap<Cell, Cell> headMap(Cell k) { return this.headMap(k, true); }

  @Override 
  public ConcurrentNavigableMap<Cell, Cell> tailMap(Cell k) { return this.tailMap(k, true); }


  // -------------------------------- Key's getters --------------------------------
  @Override public Cell firstKey() {
    if (isDescending) return lastKey();
    if (isEmpty()) return null;
    return getCellFromIndex(minCellIdx);
  }

  @Override public Cell lastKey() {
    if (isDescending) return firstKey();
    if (isEmpty()) return null;
    return getCellFromIndex(maxCellIdx-1);
  }

  @Override public Cell lowerKey(Cell k) {
    if (isDescending) return higherKey(k);
    if (isEmpty()) return null;
    int index = find(k);
    if (index >= 0) index -= 1; // There's a key exactly equal.
    else index = -(index + 1) - 1;
    if (index < minCellIdx || index >= maxCellIdx) return null;
    return getCellFromIndex(index);
  }

  @Override public Cell floorKey(Cell k) {
    if (isDescending) ceilingEntry(k);
    if (isEmpty()) return null;
    int index = find(k);
    if (index < 0) index = -(index + 1) - 1;
    if (index < minCellIdx || index >= maxCellIdx) return null;
    return getCellFromIndex(index);
  }

  @Override public Cell ceilingKey(Cell k) {
    if (isDescending) return floorKey(k);
    if (isEmpty()) return null;
    int index = find(k);
    if (index < 0)  index = -(index + 1);
    if (index < minCellIdx || index >= maxCellIdx) return null;
    return getCellFromIndex(index);
  }

  @Override public Cell higherKey(Cell k) {
    if (isDescending) return lowerKey(k);
    if (isEmpty()) return null;
    int index = find(k);
    if (index >= 0) index += 1; // There's a key exactly equal.
    else index = -(index + 1);
    if (index < minCellIdx || index >= maxCellIdx) return null;
    return getCellFromIndex(index);
  }

  @Override public boolean containsKey(Object o) {
    int index = find((Cell) o);
    return (index >= 0);
  }

  @Override public boolean containsValue(Object o) { // use containsKey(Object o) instead
    throw new UnsupportedOperationException();
  }

  @Override public Cell get(Object o) {
    int index = find((Cell) o);
    if (index >= 0) {
      return getCellFromIndex(index);
    }
    return null;
  }

  // -------------------------------- Entry's getters --------------------------------
  // all interfaces returning Entries are unsupported because we are dealing only with the keys
  @Override 
  public Entry<Cell, Cell> lowerEntry(Cell k) { throw new UnsupportedOperationException(); }

  @Override 
  public Entry<Cell, Cell> higherEntry(Cell k) { throw new UnsupportedOperationException(); }

  @Override 
  public Entry<Cell, Cell> ceilingEntry(Cell k) { throw new UnsupportedOperationException(); }

  @Override 
  public Entry<Cell, Cell> floorEntry(Cell k) { throw new UnsupportedOperationException(); }

  @Override 
  public Entry<Cell, Cell> firstEntry() { throw new UnsupportedOperationException(); }

  @Override 
  public Entry<Cell, Cell> lastEntry() { throw new UnsupportedOperationException(); }

  @Override 
  public Entry<Cell, Cell> pollFirstEntry() { throw new UnsupportedOperationException(); }

  @Override 
  public Entry<Cell, Cell> pollLastEntry() { throw new UnsupportedOperationException(); }


  // -------------------------------- Updates --------------------------------
  // All updating methods below are unsupported.
  // Assuming an array of Cells will be allocated externally,
  // fill up with Cells and provided in construction time.
  // Later the structure is immutable.
  @Override public Cell put(Cell k, Cell v) { throw new UnsupportedOperationException(); }

  @Override public void clear() { throw new UnsupportedOperationException(); }

  @Override public Cell remove(Object o) { throw new UnsupportedOperationException(); }

  @Override 
  public boolean replace(Cell k, Cell v, Cell v1) { throw new UnsupportedOperationException(); }

  @Override public void putAll(Map<? extends Cell, ? extends Cell> map) {
    throw new UnsupportedOperationException();
  }

  @Override public Cell putIfAbsent(Cell k, Cell v) { throw new UnsupportedOperationException(); }

  @Override 
  public boolean remove(Object o, Object o1) { throw new UnsupportedOperationException(); }

  @Override public Cell replace(Cell k, Cell v) { throw new UnsupportedOperationException(); }


  // -------------------------------- Sub-Sets --------------------------------
  @Override public NavigableSet<Cell> navigableKeySet() { return new CellBlocksSet(); }

  @Override 
  public NavigableSet<Cell> descendingKeySet() { throw new UnsupportedOperationException(); }

  @Override public NavigableSet<Cell> keySet() { throw new UnsupportedOperationException(); }

  @Override public Collection<Cell> values() { return new CellBlocksCollection(); }

  @Override public Set<Entry<Cell, Cell>> entrySet() { throw new UnsupportedOperationException(); }


  // -------------------------------- Iterator K --------------------------------
  private final class CellBlocksIterator implements Iterator<Cell> {
    int index = minCellIdx;

    private CellBlocksIterator(boolean d) {
//      org.junit.Assert.assertTrue("\nInitializing Cell iterator, descending?:" + d + "\n",false);

      isDescending = d;
      index = isDescending ? maxCellIdx-1 : minCellIdx;
    }

    @Override
    public boolean hasNext() {
      return isDescending ? (index >= minCellIdx) : (index < maxCellIdx);
    }

    @Override
    public Cell next() {
      Cell result = getCellFromIndex(index);
      if (isDescending) index--; else index++;
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  
  // -------------------------------- Navigable Set --------------------------------
  private final class CellBlocksSet implements NavigableSet<Cell> {

    @Override public Cell lower(Cell k)       { return lowerKey(k); }

    @Override public Cell floor(Cell k)       { return floorKey(k); }

    @Override public Cell ceiling(Cell k)     { return ceilingKey(k); }

    @Override public Cell higher(Cell k)      { return higherKey(k); }

    @Override public Cell first()          { return firstKey(); }

    @Override public Cell last()           { return lastKey(); }

    @Override public Cell pollFirst()      { throw new UnsupportedOperationException(); }

    @Override public Cell pollLast()       { throw new UnsupportedOperationException(); }

    @Override public int size()         { return size(); }

    @Override public boolean isEmpty()  { return isEmpty(); }

    @Override public void clear()       { throw new UnsupportedOperationException();  }

    @Override public boolean contains(Object o) { return containsKey(o); }

    @Override public Comparator<? super Cell> comparator() { return comparator; }

    @Override public Iterator<Cell> iterator() { return new CellBlocksIterator(false); }

    @Override public Iterator<Cell> descendingIterator() { return new CellBlocksIterator(true); }

    @Override public Object[] toArray() { throw new UnsupportedOperationException(); }

    @Override public <T> T[] toArray(T[] ts) { throw new UnsupportedOperationException(); }

    @Override public boolean add(Cell k)   { throw new UnsupportedOperationException(); }

    @Override public boolean remove(Object o) { throw new UnsupportedOperationException(); }

    @Override public boolean containsAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(Collection<? extends Cell> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean retainAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean removeAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public NavigableSet<Cell> descendingSet() { throw new UnsupportedOperationException(); }

    @Override public SortedSet<Cell> subSet(Cell k, Cell e1) { throw new UnsupportedOperationException(); }

    @Override public NavigableSet<Cell> subSet(Cell k, boolean b, Cell e1, boolean b1) {
      throw new UnsupportedOperationException();
    }

    @Override public NavigableSet<Cell> headSet(Cell k, boolean b) { // headMap should be used instead
      throw new UnsupportedOperationException();
    }

    @Override public NavigableSet<Cell> tailSet(Cell k, boolean b) { // tailMap should be used instead
      throw new UnsupportedOperationException();
    }

    @Override public SortedSet<Cell> headSet(Cell k) { // headMap should be used instead
      throw new UnsupportedOperationException();
    }

    @Override public SortedSet<Cell> tailSet(Cell k) { // tailMap should be used instead
      throw new UnsupportedOperationException();
    }


  }

  // -------------------------------- Collection --------------------------------
  private final class CellBlocksCollection implements Collection<Cell> {

    @Override public int size()         { return size(); }

    @Override public boolean isEmpty()  { return isEmpty(); }

    @Override public void clear()       { throw new UnsupportedOperationException();  }

    @Override public boolean contains(Object o) { return containsKey(o); }

    @Override public Iterator<Cell> iterator() {
      return (isDescending) ? new CellBlocksIterator(true) : new CellBlocksIterator(false);
    }

    @Override public Object[] toArray() { throw new UnsupportedOperationException(); }

    @Override public <T> T[] toArray(T[] ts) { throw new UnsupportedOperationException(); }

    @Override public boolean add(Cell k) { throw new UnsupportedOperationException(); }

    @Override public boolean remove(Object o) { throw new UnsupportedOperationException(); }

    @Override public boolean containsAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(Collection<? extends Cell> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean removeAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean retainAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }


  }

}
