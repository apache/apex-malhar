/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.*;
import com.malhartech.engine.TestSink;
import com.malhartech.stram.StramLocalCluster;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.Sum}. <p>
 *
 */
public class SumTest
{
  private static Logger log = LoggerFactory.getLogger(SumTest.class);

  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeTypeProcessing()
  {
    Sum<Double> doper = new Sum<Double>();
    Sum<Float> foper = new Sum<Float>();
    Sum<Integer> ioper = new Sum<Integer>();
    Sum<Long> loper = new Sum<Long>();
    Sum<Short> soper = new Sum<Short>();
    doper.setType(Double.class);
    foper.setType(Float.class);
    ioper.setType(Integer.class);
    loper.setType(Long.class);
    soper.setType(Short.class);

    testNodeSchemaProcessing(doper);
    testNodeSchemaProcessing(foper);
    testNodeSchemaProcessing(ioper);
    testNodeSchemaProcessing(loper);
    testNodeSchemaProcessing(soper);
  }

  public void testNodeSchemaProcessing(Sum oper)
  {
    TestSink sumSink = new TestSink();
    oper.sum.setSink(sumSink);

    oper.beginWindow(0); //

    Double a = new Double(2.0);
    Double b = new Double(20.0);
    Double c = new Double(1000.0);

    oper.data.process(a);
    oper.data.process(b);
    oper.data.process(c);

    a = 1.0;
    oper.data.process(a);
    a = 10.0;
    oper.data.process(a);
    b = 5.0;
    oper.data.process(b);

    b = 12.0;
    oper.data.process(b);
    c = 22.0;
    oper.data.process(c);
    c = 14.0;
    oper.data.process(c);

    a = 46.0;
    oper.data.process(a);
    b = 2.0;
    oper.data.process(b);
    a = 23.0;
    oper.data.process(a);

    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 1, sumSink.collectedTuples.size());
    for (Object o: sumSink.collectedTuples) { // sum is 1157
      Double val = ((Number)o).doubleValue();
      Assert.assertEquals("emitted sum value was was ", new Double(1157.0), val);
    }
  }

  /**
   * Tuple generator to test partitioning.
   */
  public static class TestInputOperator extends BaseOperator implements InputOperator
  {
    @OutputPortFieldAnnotation(name = "output")
    public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>(this);
    public transient boolean first = true;

    @Override
    public void emitTuples()
    {
      if (first) {
        for (int i = 0; i < 60; i++) {
          output.emit(new Integer(i));  // send just only one tuple
        }
        first = false;
      }
    }
  }

  /**
   * Tuple collector to test partitioning.
   */
  public static class CollectorOperator extends BaseOperator
  {
    public static final ArrayList<Integer> buffer = new ArrayList<Integer>();
    public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>(this)
    {
      @Override
      public void process(Integer tuple)
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
      Sum<Integer> oper = dag.addOperator("sum", new Sum<Integer>());
      oper.setType(Integer.class);
      CollectorOperator collector = dag.addOperator("collector", new CollectorOperator());

      dag.getMeta(oper).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(N);

      dag.addStream("test_sum", test.output, oper.data).setInline(false);
      dag.addStream("sum_console", oper.sum, collector.input).setInline(false);

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
      log.debug(String.format("sum of a value %s", CollectorOperator.buffer.get(0).toString()));
      log.debug(String.format("sum of a value %s", CollectorOperator.buffer.toString()));
      CollectorOperator.buffer.clear();
    }
    catch (Exception ex) {
      log.debug("got exception", ex);
    }
  }
}
