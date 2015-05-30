package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;

/**
 * This is the scanner for any *MemStore implementation, derived from MemStore.
 * Currently, the scanner works with DefaultMemStore and CompactMemStore.
 * The MemStoreScanner combines CellSetMgrScanners from different CellSetMgrs and
 * uses the key-value heap and the reversed key-value heap for the aggregated key-values set.
 *
 * It is assumed that only traversing forward or backward is used (without zigzagging in between)
 *
 */
@InterfaceAudience.Private
public class MemStoreScanner extends NonLazyKeyValueScanner {

    private KeyValueHeap forwardHeap;           // heap of scanners used for traversing forward
    private ReversedKeyValueHeap backwardHeap;  // reversed scanners heap for traversing backward

    private MemStoreScanType type               // The type of the scan is defined by con-r for
            = MemStoreScanType.UNDEFINED;       // compaction or according to the first usage

    private long readPoint;

    private AbstractMemStore                    // pointer back to the relevant MemStore
            backwardReferenceToMemStore;        // is needed for shouldSeek() method

    /**
     * Constructor.
     * If UNDEFINED type for MemStoreScanner is provided - the USER_FORWARD type is used as default!
     * After constructor only one heap is going to be initialized for entire lifespan
     * of the MemStoreScanner. As a specific scanner ca only be one directed.
     *
     * @param readPoint Read point below which we can safely remove duplicate KVs
     * @param type The scan type COMPACT_FORWARD should be used for compaction
     * @param ms Pointer back to the MemStore
     */
    public MemStoreScanner( AbstractMemStore ms,
                            long readPoint,
                            MemStoreScanType type) throws IOException {
        super();
        this.readPoint      = readPoint;
        this.type = type;
        switch (type){
            case UNDEFINED:
            case USER_SCAN_FORWARD:
            case COMPACT_FORWARD:
                this.forwardHeap    = new KeyValueHeap(ms.getListOfScanners(readPoint), ms.getComparator());
                break;
            case USER_SCAN_BACKWARD:
                this.backwardHeap   = new ReversedKeyValueHeap(ms.getListOfScanners(readPoint), ms.getComparator());
                break;
        }
        this.backwardReferenceToMemStore = ms;
//        if (Trace.isTracing() && Trace.currentSpan() != null) {
//            Trace.currentSpan().addTimelineAnnotation("Creating MemStoreScanner");
//        }
    }

    /* Constructor used only when the scan usage is unknown and need to be defined according to the first move */
    public MemStoreScanner(AbstractMemStore ms, long readPt) throws IOException {
        this(ms, readPt, MemStoreScanType.UNDEFINED);
    }

    /**
     * Checks whether the type of the scan suits the assumption of moving forward
     * */
    private void assertForward(){
        if(type==MemStoreScanType.UNDEFINED)
            type = MemStoreScanType.USER_SCAN_FORWARD;

        assert (type!=MemStoreScanType.USER_SCAN_BACKWARD);
    }

    /**
     * Checks whether the type of the scan suits the assumption of moving forward
     * */
    private void assertBackward() throws IOException{
        if(type==MemStoreScanType.UNDEFINED) {
            // In case we started from peek, release the forward heap
            // and build backward. Set the correct type. Thus this turn
            // can happen only once
            if ((backwardHeap==null)&&(forwardHeap!=null)){
                forwardHeap.close();
                forwardHeap = null;
                this.backwardHeap   =
                        new ReversedKeyValueHeap(   backwardReferenceToMemStore.getListOfScanners(readPoint),
                                backwardReferenceToMemStore.getComparator());
                type = MemStoreScanType.USER_SCAN_BACKWARD;
            }
        }

        assert (type!=MemStoreScanType.USER_SCAN_FORWARD);
    }

    /**
     * Returns the cell from the top-most scanner without advancing the iterator.
     * The backward traversal is assumed, only if specified explicitly
     */
    @Override
    public KeyValue peek() {
        if (type == MemStoreScanType.USER_SCAN_BACKWARD)
            return backwardHeap.peek();
        return forwardHeap.peek();
    }

    /**
     * Gets the next cell from the top-most scanner. Assumed forward scanning.
     */
    @Override
    public KeyValue next() throws IOException {
        assertForward();
        for (KeyValue currentCell = forwardHeap.next();     // loop over till the next suitable value
             currentCell != null;                       // take next value from the forward heap
             currentCell = forwardHeap.next()){

            if (currentCell.getMvccVersion() > readPoint)
                continue;                               // the value too old, take next one

            if (type == MemStoreScanType.COMPACT_FORWARD) {
                // check the returned cell, skipping this code for now as it was not supported in
                // the initial MemStoreScanner. In future implementation ScanQueryMatcher need
                // to be used
            }

            return currentCell;
        }
        return null;
    }

    /**
     *  Set the scanner at the seek key. Assumed forward scanning.
     *  Must be called only once: there is no thread safety between the scanner
     *  and the memStore.
     * @param key seek value
     * @return false if the key is null or if there is no data
     */
    @Override
    public boolean seek(KeyValue key) throws IOException {
        assertForward();

        if (key == null) {
            close();
            return false;
        }

        return forwardHeap.seek(key);
    }

    /**
     * Move forward on the sub-lists set previously by seek. Assumed forward scanning.
     * @param key seek value (should be non-null)
     * @return true if there is at least one KV to read, false otherwise
     */
    @Override
    public boolean reseek(KeyValue key) throws IOException {
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
        */
        assertForward();
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
            assert((type==MemStoreScanType.USER_SCAN_FORWARD) ||
                    (type==MemStoreScanType.COMPACT_FORWARD) || (type==MemStoreScanType.UNDEFINED));
            forwardHeap.close();
            forwardHeap = null;
            if (backwardHeap != null) {
                backwardHeap.close();
                backwardHeap = null;
            }
        }
        else if (backwardHeap != null) {
            assert (type==MemStoreScanType.USER_SCAN_BACKWARD);
            backwardHeap.close();
            backwardHeap = null;
        }
    }

    /**
     *  Set the scanner at the seek key. Assumed backward scanning.
     * @param key seek value
     * @return false if the key is null or if there is no data
     */
    @Override
    public boolean backwardSeek(KeyValue key) throws IOException {
        assertBackward();
        return backwardHeap.backwardSeek(key);
    }

    /**
     *  Assumed backward scanning.
     * @param key seek value
     * @return false if the key is null or if there is no data
     */
    @Override
    public boolean seekToPreviousRow(KeyValue key) throws IOException {
        assertBackward();
        return backwardHeap.seekToPreviousRow(key);
    }

    @Override
    public boolean seekToLastRow() throws IOException {
        // TODO: it looks like this is how it should be, however ReversedKeyValueHeap class doesn't
        // implement seekToLastRow() method :(
        // however seekToLastRow() was implemented in internal MemStoreScanner
        // so I wonder whether we need to come with our own workaround, or to update ReversedKeyValueHeap
        assertBackward();
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
