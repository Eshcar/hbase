package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * This is the scanner for any *MemStore implementation derived from MemStore,
 * currently works for DefaultMemStore and CompactMemStore.
 * The MemStoreScanner uses the set of CellSetMgrScanners
 *
 * Threads safety: It is assumed that CellSetMgr's underneath key-value set implementations is
 * lock-free, therefore no special synchronization is required for concurrent scanners.
 */
@InterfaceAudience.Private
class MemStoreScanner extends NonLazyKeyValueScanner {
    private KeyValueHeap forwardHeap;
    private ReversedKeyValueHeap backwardHeap;
    private long readPoint;

    public MemStoreScanner(List<KeyValueScanner> scanners,
                    KeyValue.KVComparator comparator,
                    long readPoint) throws IOException {
        super();
        this.readPoint = readPoint;
        this.forwardHeap = new KeyValueHeap(scanners, comparator);
        this.backwardHeap = new ReversedKeyValueHeap(scanners, comparator);
    }

    @Override
    public Cell peek() {
        return null;
    }

    @Override
    public Cell next() throws IOException {
        return null;
    }

    @Override
    public boolean seek(Cell key) throws IOException {
        return false;
    }

    @Override
    public boolean reseek(Cell key) throws IOException {
        return false;
    }

    @Override
    public long getSequenceID() {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean backwardSeek(Cell key) throws IOException {
        return false;
    }

    @Override
    public boolean seekToPreviousRow(Cell key) throws IOException {
        return false;
    }

    @Override
    public boolean seekToLastRow() throws IOException {
        return false;
    }


}
