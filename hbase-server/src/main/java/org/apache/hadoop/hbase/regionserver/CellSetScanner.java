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

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;

/**
 * A scanner of a single cell set bucket {@link CellSetMgr}.
 */
@InterfaceAudience.Private
class CellSetScanner implements KeyValueScanner{

  private final CellSetMgr cellSetMgr;    // the observed structure
  private long readPoint;
  private Iterator<KeyValue> iter;        // the current iterator that can be reinitialized by seek(),
                                          // backwardSeek(), or reseek()

  private KeyValue current = null;        // the pre-calculated cell to be returned by peek() or next()

  // A flag represents whether could stop skipping KeyValues for MVCC
  // if have encountered the next row. Only used for reversed scan
  private boolean stopSkippingKVsIfNextRow = false;

  // last iterated KVs by seek (to restore iterator state after reseek)
  private KeyValue last = null;

  /**---------------------------------------------------------
   * C-tor
   */
  public CellSetScanner(CellSetMgr cellSetMgr, long readPoint) {
    super();
    this.cellSetMgr   = cellSetMgr;
    this.readPoint    = readPoint;  // the highest relevant MVCC

    iter = cellSetMgr.iterator();
    current = getNext();            // initialize the current at the start
                                    // is required for working with heap of CellSetScanners
    //increase the reference count so the underlying structure will not be de-allocated
    this.cellSetMgr.incScannerCount();
  }


  /**---------------------------------------------------------
   * Private internal method for iterating over the CellSet
   * */
  private KeyValue getNext() {
    KeyValue startKV = current;
    KeyValue next = null;

    try {
      while (iter.hasNext()) {
        next = iter.next();
        if (next.getMvccVersion() <= this.readPoint) { // skip irrelevant versions
          return next;
        }
        if (stopSkippingKVsIfNextRow &&                // for backwardSeek() stay in the
                startKV != null &&                     // boundaries of single row
                cellSetMgr.compareRows(next, startKV) > 0) {
          return null;
        }
      } // end of while

      return null; // nothing found
    } finally {
      if (next != null) {
        // in all cases, remember the last KV we iterated to, needed for reseek()+
        last = next;
      }
    }
  }


  /**---------------------------------------------------------
   * Private internal method returns the higher of the two key values, or null
   * if they are both null
   */
  private KeyValue getHighest(KeyValue first, KeyValue second) {
    if (first == null && second == null) {
      return null;
    }
    if (first != null && second != null) {
      int compare = cellSetMgr.compare(first, second);
      return (compare > 0 ? first : second);
    }
    return (first != null ? first : second);
  }

  /**---------------------------------------------------------
   * Look at the next Cell in this scanner, but do not iterate scanner.
   *
   * @return the next Cell
   */
  @Override public KeyValue peek() {
    if (current!=null && current.getMvccVersion() > readPoint) {
      ;
      assert (false);
    }
    return current;
  }

  /**---------------------------------------------------------
   * Return the next Cell in this scanner, iterating the scanner
   *
   * @return the next Cell
   */
  @Override public KeyValue next() throws IOException {
    KeyValue oldCurrent = current;
    current = getNext();
    return oldCurrent;
  }

  /**---------------------------------------------------------
   * Seek the scanner at or after the specified KeyValue.
   *
   * @param key seek value
   * @return true if scanner has values left, false if end of scanner
   */
  @Override public boolean seek(KeyValue key) throws IOException {
    // restart iterator from new key
    iter = cellSetMgr.getCellSet().tailSet(key).iterator();
    last = null;      // last is going to be reinitialized in the next getNext() call
    current = getNext();
    return (current!=null);
  }

  /**---------------------------------------------------------
   * For Debug
   */
  public boolean checkForHigerMVCC(KeyValue key, long rPfound, long rPglobal) throws IOException {
    // restart iterator from new key
    Iterator<KeyValue> iter = cellSetMgr.getCellSet().tailSet(key).iterator();

    while (iter.hasNext()) {
      KeyValue curr = iter.next();
      //if (Bytes.compareTo(curr.getKey(), key.getKey())!=0) curr.createKeyOnly(true)
      if ((curr.getMvccVersion() > rPfound)&&
              (curr.getMvccVersion() <= rPglobal)) {
        return true;
      }
    } // end of while

    return false;
  }

  /**---------------------------------------------------------
   * Reseek the scanner at or after the specified KeyValue.
   * This method is guaranteed to seek at or after the required key only if the
   * key comes after the current position of the scanner. Should not be used
   * to seek to a key which may come before the current position.
   *
   * @param key seek value (should be non-null)
   * @return true if scanner has values left, false if end of scanner
   */
  @Override public boolean reseek(KeyValue key) throws IOException {

    /*
    * The ideal implementation for performance would use the sub skip list implicitly
    * pointed by the iterator. Unfortunately the Java API does not offer a method to
    * get it. So we remember the last keys we iterated to and restore
    * the reseeked set to at least that point.
    */
    iter = cellSetMgr.getCellSet().tailSet(getHighest(key, last)).iterator();
    current = getNext();
    return (current!=null);
  }

  /**---------------------------------------------------------
   * Get the sequence id associated with this KeyValueScanner. This is required
   * for comparing multiple files to find out which one has the latest data.
   * MemStoreScanner returns max value as sequence id because it will
   * always have the latest data among all files.
   */
  @Override public long getSequenceID() {
    return Long.MAX_VALUE;
  }

  /**---------------------------------------------------------
   * Close the KeyValue scanner.
   */
  @Override public void close() {
    this.cellSetMgr.decScannerCount();
  }

  /**---------------------------------------------------------
   * Allows to filter out scanners (both StoreFile and memstore) that we don't
   * want to use based on criteria such as Bloom filters and timestamp ranges.
   *
   * @param scan              the scan that we are selecting scanners for
   * @param columns           the set of columns in the current column family, or null if
   *                          not specified by the scan
   * @param oldestUnexpiredTS the oldest timestamp we are interested in for
   *                          this query, based on TTL
   * @return true if the scanner should be included in the query
   *
   * This functionality should be resolved in the higher level which is
   * MemStoreScanner, currently returns false as default, but IllegalStateException
   * can be thrown as well, but it is unlikely to change the signature.
   */
  @Override public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns,
      long oldestUnexpiredTS) {
    return false;
  }

  /**---------------------------------------------------------
   * Similar to {@link #seek} (or {@link #reseek} if forward is true) but only
   * does a seek operation after checking that it is really necessary for the
   * row/column combination specified by the kv parameter. This function was
   * added to avoid unnecessary disk seeks by checking row-column Bloom filters
   * before a seek on multi-column get/scan queries, and to optimize by looking
   * up more recent files first.
   *
   * @param kv
   * @param forward  do a forward-only "reseek" instead of a random-access seek
   * @param useBloom whether to enable multi-column Bloom filter optimization
   *
   * This scanner is working solely on the in-memory MemStore therefore this
   * interface is ot relevant.
   */
  @Override public boolean requestSeek(KeyValue kv, boolean forward, boolean useBloom)
      throws IOException {

    throw new IllegalStateException(
            "requestSeek cannot be called on CellSetScanner");
  }

  /**---------------------------------------------------------
   * We optimize our store scanners by checking the most recent store file
   * first, so we sometimes pretend we have done a seek but delay it until the
   * store scanner bubbles up to the top of the key-value heap. This method is
   * then used to ensure the top store file scanner has done a seek operation.
   *
   * This scanner is working solely on the in-memory MemStore and doesn't work
   * on store files, therefore this interface is ot relevant. In order not to change
   * the function signature no exception is thrown.
   */
  @Override public boolean realSeekDone() {
    return true;
  }

  /**---------------------------------------------------------
   * Does the real seek operation in case it was skipped by
   * seekToRowCol(KeyValue, boolean). Note that this function should
   * be never called on scanners that always do real seek operations (i.e. most
   * of the scanners and also this one). The easiest way to achieve this is to call
   * {@link #realSeekDone()} first.
   */
  @Override public void enforceSeek() throws IOException {
    throw new IllegalStateException(
            "enforceSeek cannot be called on CellSetScanner");
  }

  /**---------------------------------------------------------
   * @return true if this is a file scanner. Otherwise a memory scanner is
   * assumed.
   */
  @Override public boolean isFileScanner() {
    return false;
  }

  /**---------------------------------------------------------
   * Seek the scanner at or before the row of specified Cell, it firstly
   * tries to seek the scanner at or after the specified Cell, return if
   * peek KeyValue of scanner has the same row with specified Cell,
   * otherwise seek the scanner at the first Cell of the row which is the
   * previous row of specified KeyValue
   *
   * @param key seek KeyValue
   * @return true if the scanner is at the valid KeyValue, false if such
   * KeyValue does not exist
   */
  @Override public boolean backwardSeek(KeyValue key) throws IOException {
    seek(key);    // seek forward then go backward
    if (peek()==null || cellSetMgr.compareRows(peek(), key) > 0) {
      return seekToPreviousRow(key);
    }
    return true;
  }

  /**---------------------------------------------------------
   * Seek the scanner at the first Cell of the row which is the previous row
   * of specified key
   *
   * @param key seek value
   * @return true if the scanner at the first valid Cell of previous row,
   * false if not existing such Cell
   */
  @Override public boolean seekToPreviousRow(KeyValue key) throws IOException {
    // find a previous cell
    KeyValue firstKeyOnRow =
            KeyValue.createFirstOnRow(key.getRowArray(), key.getRowOffset(),
            key.getRowLength());
    SortedSet<KeyValue> cellHead =    // here the search is hidden
            cellSetMgr.headSet(firstKeyOnRow);
    KeyValue lastCellBeforeRow = cellHead.isEmpty() ? null : cellHead.last();

    if (lastCellBeforeRow == null) {  // end of recursion
      current = null;
      return false;
    }

    KeyValue firstKeyOnPreviousRow =  // find a previous row
            KeyValue.createFirstOnRow(lastCellBeforeRow.getRowArray(),
            lastCellBeforeRow.getRowOffset(), lastCellBeforeRow.getRowLength());
    stopSkippingKVsIfNextRow = true;
    // seek in order to update the iterator and current
    seek(firstKeyOnPreviousRow);
    stopSkippingKVsIfNextRow = false;

    // if nothing found or we searched beyond the needed, take one more step backward
    if ( peek()==null || cellSetMgr.compareRows(peek(), firstKeyOnPreviousRow) > 0) {
      return seekToPreviousRow(lastCellBeforeRow);
    }
    return true;
  }

  /**---------------------------------------------------------
   * Seek the scanner at the first KeyValue of last row
   *
   * @return true if scanner has values left, false if the underlying data is empty
   * @throws java.io.IOException
   */
  @Override public boolean seekToLastRow() throws IOException {
    KeyValue higherCell = cellSetMgr.isEmpty() ? null : cellSetMgr.last();
    if (higherCell == null) {
      return false;
    }

    KeyValue firstCellOnLastRow = KeyValue.createFirstOnRow(higherCell.getRowArray(),
            higherCell.getRowOffset(), higherCell.getRowLength());

    if (seek(firstCellOnLastRow)) {
      return true;
    } else {
      return seekToPreviousRow(higherCell);
    }
  }

  /**---------------------------------------------------------
   * @return the next key in the index (the key to seek to the next block)
   * if known, or null otherwise
   *
   * Not relevant for in-memory scanner
   */
  @Override public byte[] getNextIndexedKey() {
    return null;
  }

}
