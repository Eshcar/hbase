package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableGetMultiThreadedWithEagerCompaction extends
    TestAsyncTableGetMultiThreaded {

  @BeforeClass
  public static void setUp() throws Exception {
    setUp(HColumnDescriptor.MemoryCompaction.EAGER);
  }

}
