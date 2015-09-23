package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * A singleton store segment factory.
 * Generate concrete store segments.
 */
public class StoreSegmentFactory {

  static final String USEMSLAB_KEY = "hbase.hregion.memstore.mslab.enabled";
  static final boolean USEMSLAB_DEFAULT = true;
  static final String MSLAB_CLASS_NAME = "hbase.regionserver.mslab.class";

  private StoreSegmentFactory() {}
  private static StoreSegmentFactory instance = new StoreSegmentFactory();
  public static StoreSegmentFactory instance() { return instance; }

  public ImmutableSegment createImmutableSegment(final Configuration conf,
      final CellComparator comparator, long size) {
    MemStoreLAB memStoreLAB = getMemStoreLAB(conf);
    MutableSegment segment = generateMutableSegment(conf, comparator, memStoreLAB, size);
    return createImmutableSegment(conf, segment);
  }

  public ImmutableSegment createImmutableSegment(CellComparator comparator,
      long size) {
    MutableSegment segment = generateMutableSegment(null, comparator, null, size);
    return createImmutableSegment(null, segment);
  }

  public ImmutableSegment createImmutableSegment(final Configuration conf, MutableSegment segment) {
    return generateImmutableSegment(conf, segment);
  }
  public MutableSegment createMutableSegment(final Configuration conf,
      CellComparator comparator, long size) {
    MemStoreLAB memStoreLAB = getMemStoreLAB(conf);
    return generateMutableSegment(conf, comparator, memStoreLAB, size);
  }

  //****** private methods to instantiate concrete store segments **********//

  private ImmutableSegment generateImmutableSegment(final Configuration conf,
      MutableSegment segment) {
    // TBD use configuration to set type of segment
    return new ImmutableSegmentAdapter(segment);
  }
  private MutableSegment generateMutableSegment(
      final Configuration conf, CellComparator comparator, MemStoreLAB memStoreLAB, long size) {
    // TBD use configuration to set type of segment
      CellSet set = new CellSet(comparator);
      return new MutableCellSetSegment(set, memStoreLAB, size, comparator);
  }

  private MemStoreLAB getMemStoreLAB(Configuration conf) {
    MemStoreLAB memStoreLAB = null;
    if (conf.getBoolean(USEMSLAB_KEY, USEMSLAB_DEFAULT)) {
      String className = conf.get(MSLAB_CLASS_NAME, HeapMemStoreLAB.class.getName());
      memStoreLAB = ReflectionUtils.instantiateWithCustomCtor(className,
          new Class[] { Configuration.class }, new Object[] { conf });
      //memStoreLAB = new MemStoreLAB(conf, MemStoreChunkPool.getPool(conf));
    }
    return memStoreLAB;
  }

}
