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
package org.apache.hadoop.hbase.regionserver;

import java.util.Collection;
import java.util.HashSet;

/**
 * A {@link FlushPolicy} that only flushes store larger a given threshold. If no store is large
 * enough, then all stores will be flushed.
 * Gives priority to selecting stores with non-compacting memstores first, and only if no other
 * option, selects compacting memstores.
 */
public class FlushNonCompactingStoresFirstPolicy extends FlushLargeStoresPolicy {

  private Collection<Store> nonCompactingStores = new HashSet<>();
  private Collection<Store> compactingStores = new HashSet<>();

  /**
   * @return the stores need to be flushed.
   */
  @Override public Collection<Store> selectStoresToFlush() {
    Collection<Store> specificStoresToFlush = new HashSet<Store>();
    for(Store store : nonCompactingStores) {
      if(shouldFlush(store) || region.shouldFlushStore(store)) {
        specificStoresToFlush.add(store);
      }
    }
    if(!specificStoresToFlush.isEmpty()) return specificStoresToFlush;
    for(Store store : compactingStores) {
      if(shouldFlush(store)) {
        specificStoresToFlush.add(store);
      }
    }
    if(!specificStoresToFlush.isEmpty()) return specificStoresToFlush;
    return region.stores.values();
  }

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    this.flushSizeLowerBound = getFlushSizeLowerBound(region);
    for(Store store : region.stores.values()) {
      if(store.getMemStore().isCompactingMemStore()) {
        compactingStores.add(store);
      } else {
        nonCompactingStores.add(store);
      }
    }
  }
}
