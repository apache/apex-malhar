/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.lib.util.KeyValPair;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.stram.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance test for {@link com.malhartech.lib.testbench.SeedEventGenerator}<p>
 * <br>
 * Four keys are sent in at a high throughput rate and the classification is expected to be cover all combinations<br>
 * <br>
 * Benchmarks: A total of 40 million tuples are pushed in each benchmark<br>
 * String schema does about 1.5 Million tuples/sec<br>
 * SeedEventGenerator.valueData schema is about 4 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 */
public class SeedEventGeneratorBenchmark
{
  private static Logger log = LoggerFactory.getLogger(SeedEventGeneratorBenchmark.class);
  static int sdlist = 0;
  static int vdlist = 0;
  static int vallist = 0;
  static int kvlist = 0;

  void clear()
  {
    sdlist = 0;
    vdlist = 0;
    vallist = 0;
    kvlist = 0;
  }

  public static class CollectorOperator extends BaseOperator
  {
    public final transient DefaultInputPort<HashMap<String, String>> sdata = new DefaultInputPort<HashMap<String, String>>(this)
    {
      @Override
      public void process(HashMap<String, String> tuple)
      {
        sdlist++;
      }
    };
    public final transient DefaultInputPort<HashMap<String, String>> vdata = new DefaultInputPort<HashMap<String, String>>(this)
    {
      @Override
      public void process(HashMap<String, String> tuple)
      {
        vdlist++;
      }
    };
    public final transient DefaultInputPort<HashMap<String, ArrayList<Integer>>> vlist = new DefaultInputPort<HashMap<String, ArrayList<Integer>>>(this)
    {
      @Override
      public void process(HashMap<String, ArrayList<Integer>> tuple)
      {
        vallist++;
      }
    };

    public final transient DefaultInputPort<HashMap<String, ArrayList<KeyValPair>>> kvpair = new DefaultInputPort<HashMap<String, ArrayList<KeyValPair>>>(this)
    {
      @Override
      public void process(HashMap<String, ArrayList<KeyValPair>> tuple)
      {
        kvlist++;
      }
    };
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testSchemaNodeProcessing(true);
    testSchemaNodeProcessing(false);
  }

  @SuppressWarnings("SleepWhileInLoop")
  public void testSchemaNodeProcessing(boolean doseedkey) throws Exception
  {
    LogicalPlan dag = new LogicalPlan();

    SeedEventGenerator node = dag.addOperator("seedeventgen", SeedEventGenerator.class);
    CollectorOperator collector = dag.addOperator("data collector", new CollectorOperator());
    clear();
    if (doseedkey) {
      node.addKeyData("x", 0, 9);
    }
    dag.addStream("string_data", node.string_data, collector.sdata).setInline(true);
    dag.addStream("val_data", node.val_data, collector.vdata).setInline(true);
    dag.addStream("vallist_data", node.val_list, collector.vlist).setInline(true);
    dag.addStream("keyval_data", node.keyvalpair_list, collector.kvpair).setInline(true);

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread()
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(10000);
          lc.shutdown();
        }
        catch (InterruptedException ex) {
          log.debug("Interrupted", ex);
        }
      }
    }.start();

    lc.run();
    log.debug(String.format("\n********************************************\n%s: Got s(%d), v(%d), vd(%d), kv(%d)\n********************************************\n",
                            doseedkey ? "Key seeded" : "Key not seeded", sdlist, vallist, vdlist, kvlist));
  }
}
