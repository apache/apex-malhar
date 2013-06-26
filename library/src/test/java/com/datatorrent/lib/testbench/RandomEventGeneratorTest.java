/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.testbench;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import java.util.logging.Level;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.datatorrent.lib.testbench.RandomEventGenerator}<p>
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
public class RandomEventGeneratorTest
{
  private static Logger log = LoggerFactory.getLogger(RandomEventGeneratorTest.class);
  static int icount = 0;
  static int imax = -1;
  static int imin = -1;
  static int scount = 0;
  static int smax = -1;
  static int smin = -1;

  public static class CollectorOperator extends BaseOperator
  {
    public final transient DefaultInputPort<String> sdata = new DefaultInputPort<String>()
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
    public final transient DefaultInputPort<Integer> idata = new DefaultInputPort<Integer>()
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
  public void testNodeProcessing() throws Exception
  {
    testSchemaNodeProcessing(true, true);
    testSchemaNodeProcessing(true, false);
    testSchemaNodeProcessing(false, true);
  }

  public void testSchemaNodeProcessing(boolean dostring, boolean dointeger) throws Exception
  {
    if (!dostring && !dointeger) {
      return; // at least one has to be used
    }
    LogicalPlan dag = new LogicalPlan();
    RandomEventGenerator node = dag.addOperator("randomgen", new RandomEventGenerator());
    CollectorOperator collector = dag.addOperator("data collector", new CollectorOperator());
    clear();

    if (dostring) {
      dag.addStream("tests", node.string_data, collector.sdata).setInline(true);
    }

    if (dointeger) {
      dag.addStream("testi", node.integer_data, collector.idata).setInline(true);
    }

    node.setMinvalue(0);
    node.setMaxvalue(999);
    node.setTuplesBlast(5000);

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
          log.debug("Interrupted", ex);
        }
      }
    }.start();

    lc.run();
    log.debug(String.format("\nProcessed %d string tuples at range(%d,%d) and %d integer tuples at range(%d,%d)", scount, smax, smin, icount, imax, imin));
  }
}
