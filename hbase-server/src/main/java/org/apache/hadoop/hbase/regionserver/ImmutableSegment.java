/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.Cell;

/**
 */
public abstract class ImmutableSegment extends StoreSegment {

  @Override
  /**
   * Immutable store segment can never rollback
   */
  public long rollback(Cell cell) {
    return 0;
  }

  public CellSet getCellSet() {
    throw new NotImplementedException("Immutable Segment does not support this operation by " +
        "default");
  }

  public abstract KeyValueScanner getScannerForMemStoreSnapshot();


}
