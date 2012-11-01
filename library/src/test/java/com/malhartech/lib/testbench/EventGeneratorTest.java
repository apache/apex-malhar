/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.stram.StramLocalCluster;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.testbench.EventGenerator}. <p>
 * <br>
 * Load is generated and the tuples are outputted to ensure that the numbers are roughly in line with the weights<br>
 * <br>
 * Benchmarks:<br>
 * String schema generates over 11 Million tuples/sec<br>
 * HashMap schema generates over 1.7 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 *
 */
public class EventGeneratorTest
{
  private static Logger log = LoggerFactory.getLogger(EventGeneratorTest.class);
  static int scount = 0;
  static int hcount = 0;
  static int ccount = 0;

  /**
   * Test node logic emits correct results
   */
  public static class CollectorOperator extends BaseOperator
  {
    public final transient DefaultInputPort<String> sdata = new DefaultInputPort<String>(this)
    {
      @Override
      public void process(String tuple)
      {
        scount++;
      }
    };
    public final transient DefaultInputPort<HashMap<String, Double>> hdata = new DefaultInputPort<HashMap<String, Double>>(this)
    {
      @Override
      public void process(HashMap<String, Double> tuple)
      {
        hcount++;
      }
    };
    public final transient DefaultInputPort<HashMap<String, Number>> count = new DefaultInputPort<HashMap<String, Number>>(this)
    {
      @Override
      public void process(HashMap<String, Number> tuple)
      {
        ccount++;
        for (Map.Entry<String, Number> e: tuple.entrySet()) {
          log.debug(String.format("Count data \"%s\" = %f", e.getKey(), e.getValue().doubleValue()));
        }
      }
    };
  }

  /**
   * Tests both string and non string schema
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testSingleSchemaNodeProcessing(true); // 15 million/s
    testSingleSchemaNodeProcessing(false); // 7.5 million/s
  }

  public void testSingleSchemaNodeProcessing(boolean stringschema) throws Exception
  {
    DAG dag = new DAG();
    EventGenerator node = dag.addOperator("eventgen", EventGenerator.class);
    CollectorOperator collector = dag.addOperator("data collector", new CollectorOperator());

    node.setKeys("a,b,c,d");
    node.setValues("");
    node.setWeights("10,40,20,30");
    node.setTuplesBlast(1000);
    node.setRollingWindowCount(5);

    if (stringschema) {
      dag.addStream("stest", node.string_data, collector.sdata).setInline(true);
    }
    else {
      dag.addStream("htest", node.hash_data, collector.hdata).setInline(true);
    }
    dag.addStream("hcest", node.count, collector.count).setInline(true);

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread()
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
          lc.shutdown();
        }
        catch (InterruptedException ex) {
          java.util.logging.Logger.getLogger(EventGeneratorBenchmark.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    }.start();

    lc.run();
    log.debug(String.format("\nProcessed %d string tuples", scount));
    log.debug(String.format("\nProcessed %d hash tuples", hcount));
    log.debug(String.format("\nGot %d count tuples", ccount));
  }
}
