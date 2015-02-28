/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
