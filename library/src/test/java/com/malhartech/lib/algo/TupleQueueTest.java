/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.OperatorContext;
import com.malhartech.dag.Tuple;
import com.malhartech.lib.testbench.*;
import com.malhartech.stream.StramTestSupport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class TupleQueueTest
{
  private static Logger LOG = LoggerFactory.getLogger(EventGenerator.class);

  class QueueSink implements Sink
  {
    int count = 0;
    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        HashMap<String, Object> tuple = (HashMap<String, Object>)payload;
        for (Map.Entry<String, Object> e: tuple.entrySet()) {
          count++;
        }
      }
    }
  }

  class ConsoleSink implements Sink
  {
    int count = 0;
    HashMap<String, ArrayList> map  = new HashMap<String, ArrayList>();
    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        HashMap<String, ArrayList> tuple = (HashMap<String, ArrayList>)payload;
        for (Map.Entry<String, ArrayList> e: tuple.entrySet()) {
          map.put(e.getKey(), e.getValue());
          count++;
        }
      }
    }

    public String print() {
      String line = "\nTotal of ";
      line += String.format("%d tuples\n", count);
      for (Map.Entry<String, ArrayList> e: map.entrySet()) {
        line += e.getKey() +": ";
        for (Object o : e.getValue()) {
          line += ",";
          line += o.toString();
        }
        line += "\n";
      }
      return line;
    }
  }


  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    final TupleQueue node = new TupleQueue();

    QueueSink queueSink = new QueueSink();
    ConsoleSink consoleSink = new ConsoleSink();

    Sink dataSink = node.data.getSink();
    Sink querySink = node.query.getSink();
    node.queue.setSink(queueSink);
    node.console.setSink(consoleSink);
    node.setup(new OperatorConfiguration());

    Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
    dataSink.process(bt);
    querySink.process(bt);

    HashMap<String, Integer> dinput = null;
    int numtuples = 100;
    for (int i = 0; i < numtuples; i++) {
      dinput = new HashMap<String, Integer>();
      dinput.put("a", new Integer(i));
      dinput.put("b", new Integer(100+i));
      dinput.put("c", new Integer(200+i));
      dataSink.process(dinput);
    }
    Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
    dataSink.process(et);
    querySink.process(et);

    bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 2);
    dataSink.process(bt);
    querySink.process(bt);

    String key = "a";
    querySink.process(key);
    key = "b";
    querySink.process(key);
    key = "c";
    querySink.process(key);

    et = StramTestSupport.generateEndWindowTuple("doesn't matter", 2, 1);
    dataSink.process(et);
    querySink.process(et);

    Thread.sleep(100);
    LOG.debug(String.format("\n*************************\nQueue had %d tuples\n", queueSink.count));
    LOG.debug(consoleSink.print());
  }
}
