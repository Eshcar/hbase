package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * The ongoing MemStore Compaction manager, dispatches a solo running compaction
 * and interrupts the compaction if requested.
 * The MemStoreScanner is used to traverse the compaction pipeline. The MemStoreScanner
 * is included in internal store scanner, where all compaction logic is implemented.
 *
 * Threads safety: It is assumed that the compaction pipeline is immutable,
 * therefore no special synchronization is required.
 *
 */
@InterfaceAudience.Private
class MemStoreCompactor {
    private static final Log LOG = LogFactory.getLog(MemStoreCompactor.class);

    private CompactionPipeline  cp;             // the subject for compaction
    private CompactedMemStore   ms;             // backward reference
    private MemStoreScanner     scanner;        // scanner for pipeline only

    private InternalScanner compactingScanner;  // scanner on top of MemStoreScanner
                                                // that uses ScanQueryMatcher
    private int                                 // the limit for the scan
            compactionKVMax;
    private long                                // smallest read point for any ongoing
            smallestReadPoint;                  // MemStore scan
    private VersionedCellSetMgrList             // a static version of the CellSetMgrs
            versionedList;                      // list from the pipeline
    private final KeyValue.KVComparator comparator;

    private Thread workerThread = null;
    private final AtomicBoolean inCompaction = new AtomicBoolean(false);


    /**----------------------------------------------------------------------
    * The constructor is used only to initialize basics, other parameters
    * needing to start compaction will come with doCompact()
    * */
    public MemStoreCompactor (
        CompactedMemStore ms,
        CompactionPipeline cp,
        KeyValue.KVComparator comparator,
        Configuration conf) {

        this.ms = ms;
        this.cp = cp;
        this.comparator = comparator;
        compactionKVMax = conf.getInt(          // get the limit to the size of the
                HConstants.COMPACTION_KV_MAX,   // groups to be returned by compactingScanner
                HConstants.COMPACTION_KV_MAX_DEFAULT);
    }


    /**----------------------------------------------------------------------
    * The request to dispatch the compaction asynchronous task.
    * The method returns true if compaction was successfully dispatched, or false if there
    * is already an ongoing compaction (or pipeline is empty).
    * */
    public boolean doCompact(Store store) throws IOException {
        if (cp.isEmpty()) return false;         // no compaction on empty pipeline

        List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
        this.versionedList =                    // get the list of CellSetMgrs from the pipeline
                cp.getVersionedList();          // the list is marked with specific version

        // create the list of scanners with maximally possible read point, meaning that
        // all KVs are going to be returned by the pipeline traversing
        for (CellSetMgr mgr : this.versionedList.getCellSetMgrList()) {
          scanners.add(mgr.getScanner(Long.MAX_VALUE));
        }
        scanner = new MemStoreScanner(ms,
                scanners, Long.MAX_VALUE, MemStoreScanType.COMPACT_FORWARD);

        smallestReadPoint = store.getSmallestReadPoint();
        compactingScanner = createScanner(store);

        if (workerThread == null) {             // dispatch
            Runnable worker = new Worker();
            workerThread = new Thread(worker);
            LOG.info("Starting the MemStore in-memory compaction, with thread:" + workerThread);
            workerThread.start();
            inCompaction.set(true);
            return true;
        }
        return false;
    }

    /*----------------------------------------------------------------------
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

  /*----------------------------------------------------------------------
  * The worker thread performs the compaction asynchronously.
  * The solo (per compactor) thread only reads the compaction pipeline
  */
    private class Worker implements Runnable {

        @Override
        public void run() {
          CellSetMgr resultCellSetMgr =
              CellSetMgr.Factory.instance().createCellSetMgr(CellSetMgr.Type.COMPACTED_READ_ONLY,
                  comparator, CompactedMemStore.DEEP_OVERHEAD_PER_PIPELINE_ITEM);
            // the compaction processing
          KeyValue cell;
            try {
                // Phase I: create the compacted CellSetMgr
                compactToCellSetMgr(resultCellSetMgr);
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

    /**
     * Creates the scanner for compacting the pipeline.
     * @return the scanner
     */
    private InternalScanner createScanner(Store store) throws IOException {
        InternalScanner internalScanner = null;

        Scan scan = new Scan();
        scan.setMaxVersions();  //Get all available versions

        internalScanner = new StoreScanner(store, store.getScanInfo(), scan,
                    Collections.singletonList(scanner), ScanType.COMPACT_RETAIN_DELETES,
                    smallestReadPoint, HConstants.OLDEST_TIMESTAMP);

        return internalScanner;
    }

    /**
     * Creates a single CellSetMgr using the internal store scanner,
     * who in turn uses ScanQueryMatcher
     */
    private void compactToCellSetMgr(CellSetMgr resultCellSetMgr) throws IOException {

        List<Cell> kvs = new ArrayList<Cell>();

        boolean hasMore;
        do {
            hasMore = compactingScanner.next(kvs, compactionKVMax);
            if (!kvs.isEmpty()) {
                for (Cell c : kvs) {
                    // If we know that this KV is going to be included always, then let us
                    // set its memstoreTS to 0. This will help us save space when writing to
                    // disk.
                    KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                    // changed relatively to memstore->disc flushing
                    KeyValue newKV = resultCellSetMgr.maybeCloneWithAllocator(kv);
                    if (kv.getMvccVersion() <= smallestReadPoint) {
                        // let us not change the original KV. It could be in the memstore
                        // changing its memstoreTS could affect other threads/scanners.
                        //kv = kv.shallowCopy();
                        //kv.setMvccVersion(0);
                    }
                    resultCellSetMgr.add(newKV);

                }
                kvs.clear();
            }
        } while (hasMore && (!Thread.currentThread().isInterrupted()));


    }

  // methods for tests
  void toggleCompaction(boolean on) {
    if(on) {
      workerThread = null;
    } else {
      workerThread = new Thread();
    }
  }


}
