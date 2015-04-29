package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/**
 * The ongoing MemStore compaction manager, dispatches a solo running compaction
 * and interrupts the compaction if requested.
 * The MemStoreScanner is used to traverse the compaction pipeline
 *
 * Threads safety: It is assumed that the compaction pipeline is immutable,
 * therefore no special synchronization is required.
 */
@InterfaceAudience.Private
class MemStoreCompactor {
    private CompactionPipeline cp;
    private MemStoreScanner scanner;
    private VersionedCellSetMgrList versionedList;
    private long readPoint;

    public MemStoreCompactor (CompactionPipeline cp,
                              KeyValue.KVComparator comparator,
                              long readPoint) throws IOException{
        this.cp = cp;
        this.readPoint = readPoint;
        this.versionedList = cp.getCellSetMgrList();

        ArrayList<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();

        for (CellSetMgr mgr : this.versionedList.getCellSetMgrList()) {
            scanners.add(mgr.getScanner(readPoint));
        }

        scanner = new MemStoreScanner(scanners,comparator,readPoint, ScanType.COMPACT_DROP_DELETES);
    }

    public boolean doCompact () {
        return false;
    }

    public boolean stopCompact() {
        return false;
    }

    /*
    * The worker thread performs the compaction asynchronously
    * The thread only reads the compaction pipeline
    */
    private static class Worker implements Runnable {

        public Worker() {

        }

        @Override
        public void run() {

        }
    }
}
