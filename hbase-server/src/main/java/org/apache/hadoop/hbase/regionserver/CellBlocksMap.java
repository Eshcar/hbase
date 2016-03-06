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

import org.apache.hadoop.hbase.Cell;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * BlocksMap stores a constant number of elements and is immutable after creation stage.
 * Due to being immutable the BlocksMap can be implemented as array.
 * The BlocksMap uses no synchronization primitives, it is assumed to be created by a
 * single thread and then it can be read-only by multiple threads.
 */
public class CellBlocksMap<K,V> implements ConcurrentNavigableMap<K,V> {

  private final Comparator<? super K> comparator;
  private K blocks[];             // with hope to transfer to direct NIO byte buffers

  private int minCellIdx   = 0;   // the index of the minimal cell (for sub-sets)
  private int maxCellIdx   = 0;   // the index of the maximal cell (for sub-sets)
  private boolean isDescending = false; // array can be easily traversed backward in descending order


  public CellBlocksMap(Class<K> c, int maxSize, Comparator<? super K> comparator) {
    this.comparator = comparator;
    // Use Array native method to create array of a type only known at run time
    final K[] b = (K[]) Array.newInstance(c,maxSize);
    this.blocks = b;
    this.maxCellIdx = b.length;
  }

  public CellBlocksMap(Comparator<? super K> comparator, K[] b, int min, int max, boolean d){
    this.comparator = comparator;
    this.blocks = b;
    this.minCellIdx = min;
    this.maxCellIdx = max;
    this.isDescending = d;
  }

  /**
   * Binary search for a given key in between given boundaries of the array
   * @param needle The key to look for in all of the entries
   * @return Same return value as Arrays.binarySearch.
   * Positive numbers mean the index.
   * Otherwise (-1 * insertion point) - 1
   */
  private int find(K needle) {
    int begin = minCellIdx;
    int end = maxCellIdx - 1;

    while (begin <= end) {
      int mid = begin + ((end - begin) / 2);
      K midCell = blocks[mid];
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

  @Override public Comparator<? super K> comparator() { return comparator; }

  @Override public int size() { return maxCellIdx-minCellIdx; }

  @Override public boolean isEmpty() { return (maxCellIdx==minCellIdx); }


  // ---------------- Sub-Maps ----------------
  @Override public ConcurrentNavigableMap<K, V> subMap( K fromKey,
                                                        boolean fromInclusive,
                                                        K toKey,
                                                        boolean toInclusive) {
    int toIndex = find(toKey);
    if (toInclusive && toIndex >= 0) toIndex++;
    else if (toIndex < 0) toIndex = -(toIndex + 1) - 1;

    int fromIndex = find(fromKey);
    if (!fromInclusive && fromIndex >= 0) fromIndex++;
    else if (fromIndex < 0) fromIndex = -(fromIndex + 1);

    return new CellBlocksMap<>(comparator,blocks,fromIndex,toIndex,false);
  }

  @Override public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    int index = find(toKey);
    if (inclusive && index >= 0) index++;
    else if (index < 0) index = -(index + 1) - 1;
    return new CellBlocksMap<>(comparator,blocks,minCellIdx,index,false);
  }

  @Override public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    int index = find(fromKey);
    if (!inclusive && index >= 0) index++;
    else if (index < 0) index = -(index + 1);
    return new CellBlocksMap<>(comparator,blocks,index,maxCellIdx,false);
  }

  @Override public ConcurrentNavigableMap<K, V> descendingMap() {
    return new CellBlocksMap<>(comparator,blocks,minCellIdx,maxCellIdx,true);
  }

  @Override public ConcurrentNavigableMap<K, V> subMap(K k, K k1) {
    return this.subMap(k, true, k1, true);
  }

  @Override public ConcurrentNavigableMap<K, V> headMap(K k) { return this.headMap(k, true); }

  @Override public ConcurrentNavigableMap<K, V> tailMap(K k) { return this.tailMap(k, true); }


  // -------------------------------- Key's getters --------------------------------
  @Override public K firstKey() {
    if (isDescending) return lastKey();
    if (isEmpty()) return null;
    return blocks[minCellIdx];
  }

  @Override public K lastKey() {
    if (isDescending) return firstKey();
    if (isEmpty()) return null;
    return blocks[maxCellIdx-1];
  }

  @Override public K lowerKey(K k) {
    if (isDescending) return higherKey(k);
    if (isEmpty()) return null;
    int index = find(k);
    if (index >= 0) index -= 1; // There's a key exactly equal.
    else index = -(index + 1) - 1;
    if (index < minCellIdx || index >= maxCellIdx) return null;
    return blocks[index];
  }

  @Override public K floorKey(K k) {
    if (isDescending) ceilingEntry(k);
    if (isEmpty()) return null;
    int index = find(k);
    if (index < 0) index = -(index + 1) - 1;
    if (index < minCellIdx || index >= maxCellIdx) return null;
    return blocks[index];
  }

  @Override public K ceilingKey(K k) {
    if (isDescending) return floorKey(k);
    if (isEmpty()) return null;
    int index = find(k);
    if (index < 0)  index = -(index + 1);
    if (index < minCellIdx || index >= maxCellIdx) return null;
    return blocks[index];
  }

  @Override public K higherKey(K k) {
    if (isDescending) return lowerKey(k);
    if (isEmpty()) return null;
    int index = find(k);
    if (index >= 0) index += 1; // There's a key exactly equal.
    else index = -(index + 1);
    if (index < minCellIdx || index >= maxCellIdx) return null;
    return blocks[index];
  }

  @Override public boolean containsKey(Object o) {
    int index = find((K) o);
    return (index >= 0);
  }

  @Override public boolean containsValue(Object o) { // use containsKey(Object o) instead
    throw new UnsupportedOperationException();
  }

  @Override public V get(Object o) {
    int index = find((K) o);
    if (index >= 0) {
      return (V)blocks[index];
    }
    return null;
  }

  // -------------------------------- Entry's getters --------------------------------
  // all interfaces returning Entries are unsupported because we are dealing only with the keys
  @Override public Entry<K, V> lowerEntry(K k) { throw new UnsupportedOperationException(); }

  @Override public Entry<K, V> higherEntry(K k) { throw new UnsupportedOperationException(); }

  @Override public Entry<K, V> ceilingEntry(K k) { throw new UnsupportedOperationException(); }

  @Override public Entry<K, V> floorEntry(K k) { throw new UnsupportedOperationException(); }

  @Override public Entry<K, V> firstEntry() { throw new UnsupportedOperationException(); }

  @Override public Entry<K, V> lastEntry() { throw new UnsupportedOperationException(); }

  @Override public Entry<K, V> pollFirstEntry() { throw new UnsupportedOperationException(); }

  @Override public Entry<K, V> pollLastEntry() { throw new UnsupportedOperationException(); }


  // -------------------------------- Updates --------------------------------
  // All updating methods below are unsupported.
  // Assuming an array of Cells will be allocated externally,
  // fill up with Cells and provided in construction time.
  // Later the structure is immutable.
  @Override public V put(K k, V v) { throw new UnsupportedOperationException(); }

  @Override public void clear() { throw new UnsupportedOperationException(); }

  @Override public V remove(Object o) { throw new UnsupportedOperationException(); }

  @Override public boolean replace(K k, V v, V v1) { throw new UnsupportedOperationException(); }

  @Override public void putAll(Map<? extends K, ? extends V> map) {
    throw new UnsupportedOperationException();
  }

  @Override public V putIfAbsent(K k, V v) { throw new UnsupportedOperationException(); }

  @Override public boolean remove(Object o, Object o1) { throw new UnsupportedOperationException(); }

  @Override public V replace(K k, V v) { throw new UnsupportedOperationException(); }


  // -------------------------------- Sub-Sets --------------------------------
  @Override public NavigableSet<K> navigableKeySet() { return new CellBlocksSet(); }

  @Override public NavigableSet<K> descendingKeySet() { throw new UnsupportedOperationException(); }

  @Override public NavigableSet<K> keySet() { throw new UnsupportedOperationException(); }

  @Override public Collection<V> values() { return new CellBlocksCollection(); }

  @Override public Set<Entry<K, V>> entrySet() { throw new UnsupportedOperationException(); }


  // -------------------------------- Iterator K --------------------------------
  private final class CellBlocksIteratorK implements Iterator<K> {
    int index;

    private CellBlocksIteratorK(boolean d) {
      isDescending = d;
      index = isDescending ? maxCellIdx-1 : minCellIdx;
    }

    @Override
    public boolean hasNext() {
      return isDescending ? (index >= minCellIdx) : (index < maxCellIdx);
    }

    @Override
    public K next() {
      K result = blocks[index];
      if (isDescending) index--; else index++;
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  // -------------------------------- Iterator V --------------------------------
  private final class CellBlocksIteratorV implements Iterator<V> {
    int index;

    private CellBlocksIteratorV(boolean d) {
      isDescending = d;
      index = isDescending ? maxCellIdx-1 : minCellIdx;
    }

    @Override
    public boolean hasNext() {
      return isDescending ? (index >= minCellIdx) : (index < maxCellIdx);
    }

    @Override
    public V next() {
      V result = (V) blocks[index];
      if (isDescending) index--; else index++;
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  // -------------------------------- Navigable Set --------------------------------
  private final class CellBlocksSet implements NavigableSet<K> {

    @Override public K lower(K k)       { return lowerKey(k); }

    @Override public K floor(K k)       { return floorKey(k); }

    @Override public K ceiling(K k)     { return ceilingKey(k); }

    @Override public K higher(K k)      { return higherKey(k); }

    @Override public K first()          { return firstKey(); }

    @Override public K last()           { return lastKey(); }

    @Override public K pollFirst()      { throw new UnsupportedOperationException(); }

    @Override public K pollLast()       { throw new UnsupportedOperationException(); }

    @Override public int size()         { return size(); }

    @Override public boolean isEmpty()  { return isEmpty(); }

    @Override public void clear()       { throw new UnsupportedOperationException();  }

    @Override public boolean contains(Object o) { return containsKey(o); }

    @Override public Comparator<? super K> comparator() { return comparator; }

    @Override public Iterator<K> iterator() { return new CellBlocksIteratorK(false); }

    @Override public Iterator<K> descendingIterator() { return new CellBlocksIteratorK(true); }

    @Override public Object[] toArray() { throw new UnsupportedOperationException(); }

    @Override public <T> T[] toArray(T[] ts) { throw new UnsupportedOperationException(); }

    @Override public boolean add(K k)   { throw new UnsupportedOperationException(); }

    @Override public boolean remove(Object o) { throw new UnsupportedOperationException(); }

    @Override public boolean containsAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(Collection<? extends K> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean retainAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean removeAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public NavigableSet<K> descendingSet() { throw new UnsupportedOperationException(); }

    @Override public SortedSet<K> subSet(K k, K e1) { throw new UnsupportedOperationException(); }

    @Override public NavigableSet<K> subSet(K k, boolean b, K e1, boolean b1) {
      throw new UnsupportedOperationException();
    }

    @Override public NavigableSet<K> headSet(K k, boolean b) { // headMap should be used instead
      throw new UnsupportedOperationException();
    }

    @Override public NavigableSet<K> tailSet(K k, boolean b) { // tailMap should be used instead
      throw new UnsupportedOperationException();
    }

    @Override public SortedSet<K> headSet(K k) { // headMap should be used instead
      throw new UnsupportedOperationException();
    }

    @Override public SortedSet<K> tailSet(K k) { // tailMap should be used instead
      throw new UnsupportedOperationException();
    }


  }

  // -------------------------------- Collection --------------------------------
  private final class CellBlocksCollection implements Collection<V> {

    @Override public int size()         { return size(); }

    @Override public boolean isEmpty()  { return isEmpty(); }

    @Override public void clear()       { throw new UnsupportedOperationException();  }

    @Override public boolean contains(Object o) { return containsKey(o); }

    @Override public Iterator<V> iterator() { return new CellBlocksIteratorV(false); }

    @Override public Object[] toArray() { throw new UnsupportedOperationException(); }

    @Override public <T> T[] toArray(T[] ts) { throw new UnsupportedOperationException(); }

    @Override public boolean add(V k) { throw new UnsupportedOperationException(); }

    @Override public boolean remove(Object o) { throw new UnsupportedOperationException(); }

    @Override public boolean containsAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(Collection<? extends V> collection) {
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
