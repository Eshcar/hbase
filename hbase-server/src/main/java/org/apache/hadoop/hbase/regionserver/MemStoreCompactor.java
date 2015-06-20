package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private AbstractMemStore ms;
    private MemStoreScanner scanner;
    private VersionedCellSetMgrList versionedList;
    final KeyValue.KVComparator comparator;
    Thread workerThread = null;
    private final AtomicBoolean inCompaction = new AtomicBoolean(false);

    public MemStoreCompactor (
        AbstractMemStore ms,
        CompactionPipeline cp,
        KeyValue.KVComparator comparator) {
        this.ms = ms;
        this.cp = cp;
        this.comparator = comparator;

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
    public boolean doCompact(long readPoint) throws IOException {
        this.versionedList = cp.getVersionedList();
        List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();

        for (CellSetMgr mgr : this.versionedList.getCellSetMgrList()) {
          scanners.add(mgr.getScanner(readPoint));
        }
        scanner =
          new MemStoreScanner(ms,scanners,readPoint, MemStoreScanType.COMPACT_FORWARD);
        if (workerThread == null) {
            Runnable worker = new Worker();
            workerThread = new Thread(worker);
            workerThread.start();
            inCompaction.set(true);
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
        inCompaction.set(false);
        if (workerThread!=null) {
            workerThread.interrupt();
            workerThread=null;
        }
    }

    public boolean isInCompaction() {
        return inCompaction.get();
    }

    /*
    * The worker thread performs the compaction asynchronously.
    * The solo (per compactor) thread only reads the compaction pipeline
    */
    private class Worker implements Runnable {

        @Override
        public void run() {
          CellSetMgr resultCellSetMgr =
              CellSetMgr.Factory.instance().createCellSetMgr(CellSetMgr.Type.COMPACTED_READ_ONLY,
                  comparator, 0);
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
                if(!Thread.currentThread().isInterrupted()) {
                  cp.swap(versionedList, resultCellSetMgr);
                }
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
              stopCompact();
            }

        }
    }

}
