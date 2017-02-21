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
package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Enables compression and runs the TestWALReplay tests.
 */
@Category({ RegionServerTests.class, MediumTests.class })
@RunWith(Parameterized.class)
public class TestWALReplayCompressed extends TestWALReplay {

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] { "NONE", "BASIC", "EAGER" };
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = AbstractTestWALReplay.TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    TestWALReplay.setUpBeforeClass();
  }
}
