package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The ongoing MemStore compaction manager, dispatches a solo running compaction
 * and interrupts the compaction if requested.
 * The MemStoreScanner is used to traverse the compaction pipeline
 *
 * Threads safety: It is assumed that the compaction pipeline is immutable,
 * therefore no special synchronization is required.
 *
 * TODO: add LOG for notifications
 */
@InterfaceAudience.Private
class MemStoreCompactor {
    private CompactionPipeline cp;
    private MemStoreScanner scanner;
    private VersionedCellSetMgrList versionedList;
    private long readPoint;
    final KeyValue.KVComparator comparator;
    Thread workerThread = null;

    public MemStoreCompactor (CompactionPipeline cp,
        KeyValue.KVComparator comparator,
        long readPoint, AbstractMemStore ms) throws IOException{
        this.cp = cp;
        this.readPoint = readPoint;
        this.versionedList = cp.getVersionedList();
        this.comparator = comparator;
        ArrayList<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();

        for (CellSetMgr mgr : this.versionedList.getCellSetMgrList()) {
            scanners.add(mgr.getScanner(readPoint));
        }

        scanner =
                new MemStoreScanner(ms,readPoint, MemStoreScanType.COMPACT_FORWARD);
    }

    /*
    * The request to dispatch the compaction asynchronous task.
    * All the compaction information was provided when constructing the MemStoreCompactor
    * No input data needed here.
    * The method returns true if compaction was successfully dispatched, or false if there is
    * already an ongoing compaction.
    * TODO: Possibly to provide a compaction pipeline also to doCompact in order to allow same
    *       MemStoreCommactor instance re-usage
    */
    public boolean doCompact() {
        if (workerThread == null) {
            Runnable worker = new Worker();
            workerThread = new Thread(worker);
            workerThread.start();
            return true;
        }
        return false;
    }

    /*
    * The request to cancel the compaction asynchronous task
    * The compaction may still happen if the request was sent too late
    * Non-blocking request
    */
    public void stopCompact() {
        if (workerThread!=null) {
            workerThread.interrupt();
        }
        return;
    }

    public boolean isInCompaction() {
        return (workerThread!=null);
    }

    /*
    * The worker thread performs the compaction asynchronously.
    * The solo (per compactor) thread only reads the compaction pipeline
    */
    private class Worker implements Runnable {
        private final CellSetMgr resultCellSetMgr =
                CellSetMgr.Factory.instance().createCellSetMgr(CellSetMgr.Type.COMPACTED_READ_ONLY,
                                                                comparator, 0);

        @Override
        public void run() {
            // the compaction processing
          KeyValue cell;
            try {
                // Phase I: create the compacted CellSetMgr
                cell = scanner.next();
                while ((cell!=null) && (!Thread.currentThread().isInterrupted())) {
                    resultCellSetMgr.add(cell);
                    cell = scanner.next();
                }
                // Phase II: swap the old compaction pipeline
                cp.swap(versionedList,resultCellSetMgr);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                return;
            }

        }
    }

}
