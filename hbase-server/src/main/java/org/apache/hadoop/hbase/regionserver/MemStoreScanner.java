package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.htrace.Trace;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * This is the scanner for any *MemStore implementation derived from MemStore,
 * currently works for DefaultMemStore and CompactMemStore.
 * The MemStoreScanner combines CellSetMdgScanners from different CellSetMgrs and
 * uses the heap and the reversed heap for aggregated key-values set
 *
 */
@InterfaceAudience.Private
public class MemStoreScanner extends NonLazyKeyValueScanner {

    private KeyValueHeap forwardHeap;           // heap of scanners used for traversing forward

    private ReversedKeyValueHeap backwardHeap;  // reversed scanners heap for traversing backward

    private ScanType type;
    private long readPoint;
    final KeyValue.KVComparator comparator;

    /**
     * Constructor.
     * @param scanners The list of CellSetMgrScanners participating in the superior scan
     * @param c Comparator
     */
    public MemStoreScanner(List<KeyValueScanner> scanners,
                    final KeyValue.KVComparator c,
                    long readPoint, ScanType type) throws IOException {
        super();
        this.readPoint = readPoint;
        this.comparator = c;
        this.forwardHeap = new KeyValueHeap(scanners, comparator);
        this.backwardHeap = new ReversedKeyValueHeap(scanners, comparator);
        this.type = type;

        if (Trace.isTracing() && Trace.currentSpan() != null) {
            Trace.currentSpan().addTimelineAnnotation("Creating MemStoreScanner");
        }
    }

  public MemStoreScanner(long readPt) {
    super();
  }

  @Override
    public Cell peek() {
        return null;
    }

    @Override
    public Cell next() throws IOException {
        return null;
    }

    /**
     *  Set the scanner at the seek key.
     *  Must be called only once: there is no thread safety between the scanner
     *  and the memStore.
     * @param key seek value
     * @return false if the key is null or if there is no data
     */
    @Override
    public boolean seek(Cell key) throws IOException {
        if (key == null) {
            close();
            return false;
        }

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
        if (forwardHeap != null) {
            forwardHeap.close();
            forwardHeap = null;
        }
        if (backwardHeap != null) {
            backwardHeap.close();
            backwardHeap = null;
        }
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
