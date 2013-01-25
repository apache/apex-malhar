/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.*;
import com.malhartech.engine.TestCountAndLastTupleSink;
import com.malhartech.stram.StramLocalCluster;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.MaxMap}<p>
 *
 */
public class MaxMapTest
{
  private static Logger log = LoggerFactory.getLogger(MaxMapTest.class);

  /**
   * Test functional logic.
   */
  @Test
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new MaxMap<String, Integer>(), "integer");
    testSchemaNodeProcessing(new MaxMap<String, Double>(), "double");
    testSchemaNodeProcessing(new MaxMap<String, Long>(), "long");
    testSchemaNodeProcessing(new MaxMap<String, Short>(), "short");
    testSchemaNodeProcessing(new MaxMap<String, Float>(), "float");
  }

  /**
   * Test operator logic emits correct results for each schema.
   */
  public void testSchemaNodeProcessing(MaxMap oper, String type)
  {
    TestCountAndLastTupleSink maxSink = new TestCountAndLastTupleSink();
    oper.max.setSink(maxSink);

    oper.beginWindow(0);

    int numtuples = 10000;
    // For benchmark do -> numtuples = numtuples * 100;
    if (type.equals("integer")) {
      HashMap<String, Integer> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Integer>();
        tuple.put("a", new Integer(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("double")) {
      HashMap<String, Double> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Double>();
        tuple.put("a", new Double(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("long")) {
      HashMap<String, Long> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Long>();
        tuple.put("a", new Long(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("short")) {
      HashMap<String, Short> tuple;
      int count = numtuples / 1000; // cannot cross 64K
      for (short j = 0; j < count; j++) {
        tuple = new HashMap<String, Short>();
        tuple.put("a", new Short(j));
        oper.data.process(tuple);

      }
    }
    else if (type.equals("float")) {
      HashMap<String, Float> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Float>();
        tuple.put("a", new Float(i));
        oper.data.process(tuple);
      }
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, maxSink.count);
    Number val = ((HashMap<String, Number>)maxSink.tuple).get("a");
    if (type.equals("short")) {
      Assert.assertEquals("emitted max value was ", new Double(numtuples / 1000 - 1), val);
    }
    else {
      Assert.assertEquals("emitted max value was ", new Double(numtuples - 1), val);
    }
  }

  /**
   * Tuple generator to test partitioning.
   */
  public static class TestInputOperator extends BaseOperator implements InputOperator
  {
    @OutputPortFieldAnnotation(name = "max")
    public final transient DefaultOutputPort<HashMap<String, Integer>> output = new DefaultOutputPort<HashMap<String, Integer>>(this);
    transient boolean first = true;

    @Override
    public void emitTuples()
    {
      if (first) {
        int i = 0;
        for (; i < 60; i++) {
          HashMap<String, Integer> tuple = new HashMap<String, Integer>();
          tuple.put("a", new Integer(i));
          tuple.put("b", new Integer(i));
          tuple.put("c", new Integer(i));
          output.emit(tuple);
        }
        for (; i < 80; i++) {
          HashMap<String, Integer> tuple = new HashMap<String, Integer>();
          tuple.put("a", new Integer(i));
          tuple.put("b", new Integer(i));
          output.emit(tuple);
        }
        for (; i < 100; i++) {
          HashMap<String, Integer> tuple = new HashMap<String, Integer>();
          tuple.put("a", new Integer(i));
          output.emit(tuple);
        }
        // a = 0..99, b = 0..79, c = 0..59
        first = false;
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
      //first = true;
    }
  }
  /**
   * Tuple collector to test partitioning.
   */
  public static class CollectorOperator extends BaseOperator
  {
    public static final ArrayList<HashMap<String, Integer>> buffer = new ArrayList<HashMap<String, Integer>>();
    public final transient DefaultInputPort<HashMap<String, Integer>> input = new DefaultInputPort<HashMap<String, Integer>>(this)
    {
      @Override
      public void process(HashMap<String, Integer> tuple)
      {
        buffer.add(tuple);
      }
    };
  }

  /**
   * Test partitioning.
   *
   */
  @Test
  public void partitionTest()
  {
    try {
      DAG dag = new DAG();
      int N = 4; // number of partitions.

      TestInputOperator test = dag.addOperator("test", new TestInputOperator());
      MaxMap<String, Integer> oper = dag.addOperator("max", new MaxMap<String, Integer>());
      CollectorOperator collector = dag.addOperator("console", new CollectorOperator());

      dag.getOperatorWrapper(oper).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(N);

      dag.addStream("test_max", test.output, oper.data).setInline(false);
      dag.addStream("max_console", oper.max, collector.input).setInline(false);

      final StramLocalCluster lc = new StramLocalCluster(dag);
      lc.setHeartbeatMonitoringEnabled(false);
      new Thread()
      {
        @Override
        public void run()
        {
          try {
            Thread.sleep(10000);
          }
          catch (InterruptedException ex) {
          }
          lc.shutdown();
        }
      }.start();

      lc.run();

      Assert.assertEquals("received tuples ", 1, CollectorOperator.buffer.size());
      log.debug(String.format("max of a value %s", CollectorOperator.buffer.get(0).toString()));
    }
    catch (Exception ex) {
      log.debug("got exception", ex);
    }
  }
}
