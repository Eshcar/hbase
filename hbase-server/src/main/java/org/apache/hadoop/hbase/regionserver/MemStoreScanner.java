package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.htrace.Trace;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;

/**
 * This is the scanner for any *MemStore implementation derived from MemStore,
 * currently works for DefaultMemStore and CompactMemStore.
 * The MemStoreScanner combines CellSetMdgScanners from different CellSetMgrs and
 * uses the key-value heap and the reversed key-value heap for the aggregated key-values set.
 *
 * It is assumed only traversing forward or backward is used (without zigzagging in between)
 *
 */
@InterfaceAudience.Private
public class MemStoreScanner extends NonLazyKeyValueScanner {

    private KeyValueHeap forwardHeap;           // heap of scanners used for traversing forward
    private ReversedKeyValueHeap backwardHeap;  // reversed scanners heap for traversing backward

    private ScanType type;                      // TODO: probably better to create own type
    private long readPoint;

    // TODO: to be changed to AbstractMemStore later, to be discussed with Eshcar,
    // currently points back for shouldSeek service, though if decided to leave it
    // this way new shouldSeek() method should be added to the abstract
    private DefaultMemStore backwardReferenceToMemStore;

    /**
     * Constructor.
     * @param scanners The list of CellSetMgrScanners participating in the superior scan
     * @param c Comparator
     */
    public MemStoreScanner( List<KeyValueScanner> scanners,
                            final KeyValue.KVComparator c,
                            long readPoint,
                            ScanType type,
                            DefaultMemStore ms) throws IOException {
        super();
        this.readPoint      = readPoint;
        this.forwardHeap    = new KeyValueHeap(scanners, c);
        this.backwardHeap   = new ReversedKeyValueHeap(scanners, c);
        this.type           = type;
        this.backwardReferenceToMemStore = ms;

        if (Trace.isTracing() && Trace.currentSpan() != null) {
            Trace.currentSpan().addTimelineAnnotation("Creating MemStoreScanner");
        }
    }

    public MemStoreScanner(long readPt) {
        super();
    }

    @Override
    public Cell peek() {
        return forwardHeap.peek();
    }

    /**
     * Gets the next cell from the top-most scanner.
     */
    @Override
    public Cell next() throws IOException {

        for (Cell currentCell = forwardHeap.next();     // loop over till the next suitable value
             currentCell != null;                       // take next value from the forward heap
             currentCell = forwardHeap.next()){

            if (currentCell.getSequenceId() <= readPoint)
                continue;                               // the value too old, take next one

            if (type == ScanType.COMPACT_DROP_DELETES) {
                // check the returned cell, skipping this code for now as it was not supported in
                // the initial MemStoreScanner. In future implementation ScanQueryMatcher need
                // to be used
            }
        }
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

        return forwardHeap.seek(key);
    }

    /**
     * Move forward on the sub-lists set previously by seek.
     * @param key seek value (should be non-null)
     * @return true if there is at least one KV to read, false otherwise
     */
    @Override
    public boolean reseek(Cell key) throws IOException {
        /*
        * See HBASE-4195 & HBASE-3855 & HBASE-6591 for the background on this implementation.
        * This code is executed concurrently with flush and puts, without locks.
        * Two points must be known when working on this code:
        * 1) It's not possible to use the 'kvTail' and 'snapshot'
        *  variables, as they are modified during a flush.
        * 2) The ideal implementation for performance would use the sub skip list
        *  implicitly pointed by the iterators 'kvsetIt' and
        *  'snapshotIt'. Unfortunately the Java API does not offer a method to
        *  get it. So we remember the last keys we iterated to and restore
        *  the reseeked set to at least that point.
        *
        *  TODO: The above comment copied from the original MemStoreScanner
        *   Good point is raised, but it looks to me it is OK to continue in the same way
        *   as if something is already flushed to the disc (according to the linearization point)
        *   it is OK not to find it
        */

        return forwardHeap.reseek(key);
    }

    /**
     * MemStoreScanner returns max value as sequence id because it will
     * always have the latest data among all files.
     */
    @Override
    public long getSequenceID() {
        return Long.MAX_VALUE;
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
        return backwardHeap.backwardSeek(key);
    }

    @Override
    public boolean seekToPreviousRow(Cell key) throws IOException {
        return backwardHeap.seekToPreviousRow(key);
    }

    @Override
    public boolean seekToLastRow() throws IOException {
        // TODO: it looks like this is how it should be, however ReversedKeyValueHeap class doesn't
        // implement seekToLastRow() method :(
        // however seekToLastRow() was implemented in internal MemStoreScanner
        // so I wonder whether we need to come with our own workaround, or to update ReversedKeyValueHeap
        return backwardHeap.seekToLastRow();
    }

    /**
     * Check if this memstore may contain the required keys
     * @param scan
     * @return False if the key definitely does not exist in this Memstore
     */
    @Override
    public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns,
                                    long oldestUnexpiredTS) {

        return backwardReferenceToMemStore.shouldSeek(scan,oldestUnexpiredTS);

    }
}
