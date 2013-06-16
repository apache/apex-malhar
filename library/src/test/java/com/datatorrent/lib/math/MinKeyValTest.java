/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.*;
import com.malhartech.lib.testbench.CountAndLastTupleTestSink;
import com.malhartech.lib.util.KeyValPair;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.stram.plan.logical.LogicalPlan;

import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.MinKeyVal}. <p>
 *
 */
public class MinKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(MinKeyValTest.class);

  /**
   * Test functional logic
   */
  @Test
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new MinKeyVal<String, Integer>(), "integer");
    testSchemaNodeProcessing(new MinKeyVal<String, Double>(), "double");
    testSchemaNodeProcessing(new MinKeyVal<String, Long>(), "long");
    testSchemaNodeProcessing(new MinKeyVal<String, Short>(), "short");
    testSchemaNodeProcessing(new MinKeyVal<String, Float>(), "float");
  }

  /**
   * Test operator logic emits correct results for each schema.
   *
   */
  public void testSchemaNodeProcessing(MinKeyVal oper, String type)
  {
    CountAndLastTupleTestSink minSink = new CountAndLastTupleTestSink();
    oper.min.setSink(minSink);

    oper.beginWindow(0);

    int numtuples = 10000;
    if (type.equals("integer")) {
      for (int i = numtuples; i > 0; i--) {
        oper.data.process(new KeyValPair("a", new Integer(i)));
      }
    }
    else if (type.equals("double")) {
      for (int i = numtuples; i > 0; i--) {
        oper.data.process(new KeyValPair("a", new Double(i)));
      }
    }
    else if (type.equals("long")) {
      for (int i = numtuples; i > 0; i--) {
        oper.data.process(new KeyValPair("a", new Long(i)));
      }
    }
    else if (type.equals("short")) {
      for (short j = 1000; j > 0; j--) { // cannot cross 64K
        oper.data.process(new KeyValPair("a", new Short(j)));
      }
    }
    else if (type.equals("float")) {
      for (int i = numtuples; i > 0; i--) {
        oper.data.process(new KeyValPair("a", new Float(i)));
      }
    }

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, minSink.count);
    Number val = ((KeyValPair<String, Number>)minSink.tuple).getValue().intValue();
    if (type.equals("short")) {
      Assert.assertEquals("emitted min value was ", 1, val);
    }
    else {
      Assert.assertEquals("emitted min value was ", 1, val);
    }
  }

  /**
   * Used to test partitioning.
   */
  public static class TestInputOperator extends BaseOperator implements InputOperator
  {
    @OutputPortFieldAnnotation(name = "output")
    public final transient DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>(this);
    private transient boolean first = true;

    @Override
    public void emitTuples()
    {
      if (first) {
        for (int i = 40; i < 100; i++) {
          output.emit(new KeyValPair("a", new Integer(i)));
        }
        for (int i = 50; i < 100; i++) {
          output.emit(new KeyValPair("b", new Integer(i)));
        }
        for (int i = 60; i < 100; i++) {
          output.emit(new KeyValPair("c", new Integer(i)));
        }
        first = false;
      }
    }
  }

  public static class CollectorOperator extends BaseOperator
  {
    public static final ArrayList<KeyValPair<String, Integer>> buffer = new ArrayList<KeyValPair<String, Integer>>();
    public final transient DefaultInputPort<KeyValPair<String, Integer>> input = new DefaultInputPort<KeyValPair<String, Integer>>(this)
    {
      @Override
      public void process(KeyValPair<String, Integer> tuple)
      {
        buffer.add(new KeyValPair(tuple.getKey(), tuple.getValue()));
      }
    };
  }

  /**
   * Test partitioning.
   *
   *
   */
  @Test
  public void partitionTest()
  {
    try {
      LogicalPlan dag = new LogicalPlan();
      int N = 4; // number of partitions.

      TestInputOperator test = dag.addOperator("test", new TestInputOperator());
      MinKeyVal<String, Integer> oper = dag.addOperator("min", new MinKeyVal<String, Integer>());
      oper.setType(Integer.class);
      CollectorOperator collector = dag.addOperator("collector", new CollectorOperator());

      dag.getMeta(oper).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(N);

      dag.addStream("test_min", test.output, oper.data).setInline(false); // inline has to be false to make partition working, o/w you get assertion error in assert (nodi.isInline() == false) in StramChild.java
      dag.addStream("min_console", oper.min, collector.input).setInline(false);

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

      Assert.assertEquals("received tuples ", 3, CollectorOperator.buffer.size());
      log.debug(String.format("min of a value %s", CollectorOperator.buffer.get(0).toString()));
      log.debug(String.format("min of a value %s", CollectorOperator.buffer.get(1).toString()));
      log.debug(String.format("min of a value %s", CollectorOperator.buffer.get(2).toString()));
    }
    catch (Exception ex) {
      log.debug("got exception", ex);
    }
  }
}
