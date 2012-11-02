/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.stram.StramLocalCluster;
import java.util.logging.Level;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.RandomEventGenerator}<p>
 * <br>
 * Tests both string and integer. Sets range to 0 to 999 and generates random numbers. With millions
 * of tuple all the values are covered<br>
 * <br>
 * Benchmark: pushes as many tuples are possible<br>
 * String schema does about 3 Million tuples/sec<br>
 * Integer schema does about 7 Million tuples/sec<br>
 * <br>
 * DRC validation is done<br>
 * <br>
 */
public class RandomEventGeneratorBenchmark
{
  private static Logger log = LoggerFactory.getLogger(RandomEventGeneratorBenchmark.class);
  static int icount = 0;
  static int imax = -1;
  static int imin = -1;
  static int scount = 0;
  static int smax = -1;
  static int smin = -1;

  public static class CollectorOperator extends BaseOperator
  {
    public final transient DefaultInputPort<String> sdata = new DefaultInputPort<String>(this)
    {
      @Override
      public void process(String tuple)
      {
        scount++;
        int tval = Integer.parseInt(tuple);
        if (smin == -1) {
          smin = tval;
        }
        else if (smin > tval) {
          smin = tval;
        }
        if (smax == -1) {
          smax = tval;
        }
        else if (smax < tval) {
          smax = tval;
        }
      }
    };
    public final transient DefaultInputPort<Integer> idata = new DefaultInputPort<Integer>(this)
    {
      @Override
      public void process(Integer tuple)
      {
        icount++;
        int tval = tuple.intValue();
        if (imin == -1) {
          imin = tval;
        }
        else if (imin > tval) {
          imin = tval;
        }
        if (imax == -1) {
          imax = tval;
        }
        else if (imax < tval) {
          imax = tval;
        }
      }
    };
  }

  protected void clear()
  {
    icount = 0;
    imax = -1;
    imin = -1;
    scount = 0;
    smax = -1;
    smin = -1;
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    DAG dag = new DAG();
    RandomEventGenerator node = dag.addOperator("randomgen", new RandomEventGenerator());
    CollectorOperator collector = dag.addOperator("data collector", new CollectorOperator());
    clear();

    dag.addStream("tests", node.string_data, collector.sdata).setInline(true);
    dag.addStream("testi", node.integer_data, collector.idata).setInline(true);

    node.setMinvalue(0);
    node.setMaxvalue(999);
    node.setTuplesblast(5000);

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
    log.debug(String.format("\nProcessed %d string tuples at range(%d,%d) and %d integer tuples at range(%d,%d)", scount, smax, smin, icount, imax, imin));
  }
}
