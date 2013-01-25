/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.*;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.engine.TestCountAndLastTupleSink;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.util.KeyValPair;
import com.malhartech.stram.StramLocalCluster;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.MaxKeyVal}<p>
 *
 */
public class MaxKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(MaxKeyValTest.class);

  /**
   * Test functional logic
   */
  //@Test
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new MaxKeyVal<String, Integer>(), "integer");
    testSchemaNodeProcessing(new MaxKeyVal<String, Double>(), "double");
    testSchemaNodeProcessing(new MaxKeyVal<String, Long>(), "long");
    testSchemaNodeProcessing(new MaxKeyVal<String, Short>(), "short");
    testSchemaNodeProcessing(new MaxKeyVal<String, Float>(), "float");
  }

  /**
   * Test operator logic emits correct results for each schema.
   *
   */
  public void testSchemaNodeProcessing(MaxKeyVal oper, String type)
  {
    TestCountAndLastTupleSink maxSink = new TestCountAndLastTupleSink();
    oper.max.setSink(maxSink);

    oper.beginWindow(0);

    int numtuples = 10000;
    // For benchmark do -> numtuples = numtuples * 100;
    if (type.equals("integer")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Integer(i)));
      }
    }
    else if (type.equals("double")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Double(i)));
      }
    }
    else if (type.equals("long")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Long(i)));
      }
    }
    else if (type.equals("short")) {
      int count = numtuples / 1000; // cannot cross 64K
      for (short j = 0; j < count; j++) {
        oper.data.process(new KeyValPair("a", new Short(j)));
      }
    }
    else if (type.equals("float")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Float(i)));
      }
    }

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, maxSink.count);
    Number val = ((KeyValPair<String, Number>)maxSink.tuple).getValue();
    if (type.equals("short")) {
      Assert.assertEquals("emitted min value was ", new Double(numtuples / 1000 - 1), val);
    }
    else {
      Assert.assertEquals("emitted min value was ", new Double(numtuples - 1), val);
    }
  }

  /**
   * Used to test partitioning.
   */
  public static class TestInputOperator extends BaseOperator implements InputOperator
  {
    @OutputPortFieldAnnotation(name = "max")
    public final transient DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>(this);
    private transient boolean first = true;

    @Override
    public void emitTuples()
    {
      if (first) {
        for (int i = 0; i < 100; i++) {
          output.emit(new KeyValPair("a", new Integer(i)));
        }
        for (int i = 0; i < 80; i++) {
          output.emit(new KeyValPair("b", new Integer(i)));
        }
        for (int i = 0; i < 60; i++) {
          output.emit(new KeyValPair("c", new Integer(i)));
        }
        first = false;
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
      first = true;
    }
  }

  public static class CollectorOperator extends BaseOperator
  {
    public ArrayList<KeyValPair<String, Integer>> buffer = new ArrayList<KeyValPair<String, Integer>>();
    public final transient DefaultInputPort<KeyValPair<String, Integer>> input = new DefaultInputPort<KeyValPair<String, Integer>>(this)
    {
      @Override
      public void process(KeyValPair<String, Integer> tuple)
      {
        buffer.add(new KeyValPair(tuple.getKey(), tuple.getValue()));

        if (output.isConnected()) {
          output.emit(tuple);
        }
      }
    };
    @OutputPortFieldAnnotation(name = "output", optional = true)
    public final transient DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>(this);
  }

  /**
   * Test partitioning.
   *
   *
   */
  //@Test
  public void partitionTest()
  {
    try {
      DAG dag = new DAG();
      //dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(1);
      int N = 4; // number of partitions.

      TestInputOperator test = dag.addOperator("test", new TestInputOperator());
      MaxKeyVal<String, Integer> oper = dag.addOperator("max", new MaxKeyVal<String, Integer>());
      CollectorOperator collector = dag.addOperator("collector", new CollectorOperator());

      dag.getOperatorWrapper(oper).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(N);

      dag.addStream("test_max", test.output, oper.data).setInline(false); // inline has to be false to make partition working, o/w you get assertion error in assert (nodi.isInline() == false) in StramChild.java
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

      //Thread.sleep(100);

      Assert.assertEquals("received tuples ", 3, collector.buffer.size());

      lc.shutdown();
    }
    catch (Exception ex) {
      log.debug("got exception", ex);
    }


  }
}
