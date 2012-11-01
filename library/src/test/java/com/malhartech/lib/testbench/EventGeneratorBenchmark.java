/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stram.ManualScheduledExecutorService;
import com.malhartech.stram.StramLocalCluster;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests {@link com.malhartech.lib.testbench.EventGenerator} at a very high load with stringschema. Current peak benchmark is at 16 Million tuples/sec<p>
 * <br>
 * The benchmark results matter a lot in terms of how thread contention is handled. The test has three parts<br>
 * 1. Trigger the input generator with a very high load and no wait. Set the buffersize large enough to handle growing queued up tuples<br>
 * 2. Deactivate load generator node and drain the queue<br>
 * 3. Wait till all the queue is drained<br>
 * <br>
 * No DRC check is done on the node as this test is for benchmark only<br>
 * <br>
 * Benchmark is at 26 Million tuples/src. Once we get to real Hadoop cluster, we should increase the buffer size to handle 100x more tuples and see what the raw
 * throughput would be. Then on we would not need to force either thread to wait, or be hampered by low memory on debugging
 * environment<br>
 * <br>
 */
public class EventGeneratorBenchmark
{
  private static Logger log = LoggerFactory.getLogger(EventGeneratorBenchmark.class);
  static int count = 0;

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super(module);
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
      count++;
    }
  }

  public int getCount()
  {
    return count;
  }

  public static class CollectorOperator extends BaseOperator
  {
    public final transient CollectorInputPort<String> sdata = new CollectorInputPort<String>("sdata", this);
  }

  /**
   * Benchmark the maximum payload flow for String
   * The sink would simply ignore the payload as we are testing throughput
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    DAG dag = new DAG();
    EventGenerator node = dag.addOperator("eventgen", EventGenerator.class);
    CollectorOperator collector = dag.addOperator("data collector", CollectorOperator.class);
    dag.addStream("stest", node.string_data, collector.sdata).setInline(true);

    /*
     * public final transient DefaultOutputPort<String> string_data = new DefaultOutputPort<String>(this);
     public final transient DefaultOutputPort<HashMap<String, Double>> hash_data = new DefaultOutputPort<HashMap<String, Double>>(this);
     public final transient DefaultOutputPort<HashMap<String, Number>> count = new DefaultOutputPort<HashMap<String, Number>>(this);
     */
    int numchars = 1024;
    char[] chararray = new char[numchars + 1];
    for (int i = 0; i < numchars; i++) {
      chararray[i] = 'a';
    }
    chararray[numchars] = '\0';
    String key = new String(chararray);

    node.setKeys(key);
    node.setTuplesBlast(1000);

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
    log.debug(String.format("\nProcessed %d tuples", getCount()));
  }
}
