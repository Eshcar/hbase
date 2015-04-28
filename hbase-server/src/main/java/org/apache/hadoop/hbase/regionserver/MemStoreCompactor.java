package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

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

    public MemStoreCompactor (CompactionPipeline cp) {
        this.cp = cp;

        //TODO: create the scanner
    }

    public boolean doCompact () {
        return false;
    }

    public boolean stopCompact() {
        return false;
    }
}
