/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.lib.algo.TupleQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stream.StramTestSupport;
import java.util.ArrayList;

/**
 *
 */
public class TestTupleQueue
{
  private static Logger LOG = LoggerFactory.getLogger(TupleQueue.class);

  class FifoSink implements Sink
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

    FifoSink fifoSink = new FifoSink();
    ConsoleSink consoleSink = new ConsoleSink();

    Sink dataSink = node.connect(TupleQueue.IPORT_DATA, node);
    Sink querySink = node.connect(TupleQueue.IPORT_QUERY, node);
    node.connect(TupleQueue.OPORT_FIFO, fifoSink);
    node.connect(TupleQueue.OPORT_CONSOLE, consoleSink);


    final ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
    conf.set(TupleQueue.KEY_DEPTH, "10");


    final AtomicBoolean inactive = new AtomicBoolean(true);
    new Thread()
    {
      @Override
      public void run()
      {
        inactive.set(false);
        node.setup(conf);
        node.activate(new ModuleContext("ArithmeticFifoTestNode", this));
      }
    }.start();

    /**
     * spin while the node gets activated.
     */
    int sleeptimes = 0;
    try {
      do {
        Thread.sleep(20);
        sleeptimes++;
        if (sleeptimes > 5) {
          break;
        }

      }
      while (inactive.get());
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

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
    LOG.debug(String.format("\n*************************\nFifo had %d tuples\n", fifoSink.count));
    LOG.debug(consoleSink.print());
  }
}
