package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author eshcar
 */
public class ClientAsyncPrefetchScanner extends ClientScanner {

    // exception queue (from prefetch to main scan execution)
    private Queue<Exception> exceptionsQueue;
    // prefetch runnable object to be executed asynchronously
    private PrefetchRunnable prefetchRunnable;
    // Boolean flag to ensure only a single prefetch is running (per scan)
    // eshcar: we use atomic boolean to allow multiple concurrent scanners to
    // consume records from the same cache, but still have a single prefetcher.
    // For a single scanner thread can replace this with a native boolean.
    private AtomicBoolean isPrefetchRunning = new AtomicBoolean(false);
    // Thread pool shared by all scanners
    private static final ExecutorService pool = Executors.newCachedThreadPool();

    public ClientAsyncPrefetchScanner(Configuration conf, Scan scan, TableName tableName, HConnection connection) throws IOException {
        super(conf, scan, tableName, connection);
    }

    public ClientAsyncPrefetchScanner(Configuration conf, Scan scan, TableName tableName, HConnection connection, RpcRetryingCallerFactory rpcFactory, RpcControllerFactory controllerFactory) throws IOException {
        super(conf, scan, tableName, connection, rpcFactory, controllerFactory);
    }

    @Override
    protected void initCache() {
        // concurrent cache
        // double buffer - double cache size
        cache = new LinkedBlockingQueue<Result>(this.caching*2 + 1);
        exceptionsQueue = new ConcurrentLinkedQueue<Exception>();
        prefetchRunnable = new PrefetchRunnable();
        isPrefetchRunning = new AtomicBoolean(false);
    }

    @Override
    public Result next() throws IOException {

        try{
            handleException();

            // If the scanner is closed and there's nothing left in the cache, next is a no-op.
            if (getCacheSize() == 0 && this.closed) {
                return null;
            }

            // eshcar: TODO make this condition configurable
            if (getCacheSize() <= caching) {
                // run prefetch in the background only if no prefetch is already running
                if(!isPrefetchRunning()) {
                    if (isPrefetchRunning.compareAndSet(false, true)) {
                        pool.execute(prefetchRunnable);
                    }
                }
            }

            while (isPrefetchRunning()) {
                // prefetch running or still pending
                if (getCacheSize() > 0) {
                    return cache.poll();
                } else {
                    // (busy) wait for a record - sleep
                    Threads.sleep(1);
                }
            }

            if (getCacheSize() > 0) {
                return cache.poll();
            }

            // if we exhausted this scanner before calling close, write out the scan metrics
            writeScanMetrics();
            return null;
        }finally {
            handleException();
        }
    }

    @Override
    public void close() {
        closed = true;
        if (isPrefetchRunning()) {
            // do nothing since the async prefetch still needs this resources
        } else {
            super.close();
        }
    }

    private void handleException() throws IOException {
        //The prefetch task running in the background puts any exception it
        //catches into this exception queue.
        // Rethrow the exception so the application can handle it.
        while(!exceptionsQueue.isEmpty()) {
            Exception first = exceptionsQueue.peek();
            first.printStackTrace();
            if(first instanceof IOException) {
                throw (IOException)first;
            }
            throw (RuntimeException)first;
        }
    }

    private boolean isPrefetchRunning() {
        return isPrefetchRunning.get();
    }

    private class PrefetchRunnable implements Runnable {

        @Override
        public void run() {
            try {
                prefetch();
            } catch (Exception e) {
                exceptionsQueue.add(e);
            } finally {
                isPrefetchRunning.set(false);
            }
        }

    }


}
