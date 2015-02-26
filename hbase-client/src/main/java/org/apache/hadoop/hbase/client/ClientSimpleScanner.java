package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

import java.io.IOException;
import java.util.LinkedList;

public class ClientSimpleScanner extends ClientScanner {
    public ClientSimpleScanner(Configuration conf, Scan scan, TableName tableName, HConnection connection) throws IOException {
        super(conf, scan, tableName, connection);
    }

    public ClientSimpleScanner(Configuration conf, Scan scan, TableName tableName, HConnection connection, RpcRetryingCallerFactory rpcFactory, RpcControllerFactory controllerFactory) throws IOException {
        super(conf, scan, tableName, connection, rpcFactory, controllerFactory);
    }

    @Override
    protected void initCache() {
        cache = new LinkedList<Result>();
    }

    @Override
    public Result next() throws IOException {
        // If the scanner is closed and there's nothing left in the cache, next is a
        // no-op.
        if (cache.size() == 0 && this.closed) {
            return null;
        }
        if (cache.size() == 0) {
            prefetch();
        }
        if (cache.size() > 0) {
            return cache.poll();
        }
        // if we exhausted this scanner before calling close, write out the scan
        // metrics
        writeScanMetrics();
        return null;
    }
}
