/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stream.StramTestSupport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestInvertIndex
{
  private static Logger LOG = LoggerFactory.getLogger(SearchInvertIndex.class);

  class TestSink implements Sink
  {
    ArrayList<Object> collectedTuples = new ArrayList<Object>();

    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
        LOG.debug(payload.toString());
      }
      else {
        collectedTuples.add(payload);
      }
    }
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    final SearchInvertIndex node = new SearchInvertIndex();

    TestSink indexSink = new TestSink();

    Sink inSink = node.connect(SearchInvertIndex.IPORT_DATA, node);
    node.connect(SearchInvertIndex.OPORT_INDEX, indexSink);

    ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
    conf.setBoolean(SearchInvertIndex.KEY_PASSVALUE, false); // test with String
    node.setup(conf);

    final AtomicBoolean inactive = new AtomicBoolean(true);
    new Thread()
    {
      @Override
      public void run()
      {
        inactive.set(false);
        node.activate(new ModuleContext("ArithmeticQuotientTestNode", this));
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
        if (sleeptimes > 20) {
          break;
        }
      }
      while (inactive.get());
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

    Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
    inSink.process(bt);

    HashMap<String, ArrayList> input = new HashMap<String, ArrayList>();
    ArrayList<String> alist = new ArrayList<String>();
    alist.add(new String("str"));
    alist.add(new String("str1"));
    input.put("a", alist);
    input.put("b", alist);
    inSink.process(input);

    alist = new ArrayList<String>();
    input = new HashMap<String, ArrayList>();
    alist.add(new String("blah"));
    alist.add(new String("str1"));
    input.put("c", alist);
    inSink.process(input);

    Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
    inSink.process(et);
    try {
      for (int i = 0; i < 10; i++) {
        Thread.sleep(5);
        if (indexSink.collectedTuples.size() >= 1) {
          break;
        }
      }
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }


    node.deactivate();

    // One for each key
    Assert.assertEquals("number emitted tuples", 3, indexSink.collectedTuples.size());

    for (Object o: indexSink.collectedTuples) {
      HashMap<String, ArrayList<String>> output = (HashMap<String, ArrayList<String>>)o;
      for (Map.Entry<String, ArrayList<String>> e: output.entrySet()) {
        String lstr = "Key " + e.getKey() + ": ";
        alist = e.getValue();
        if (alist == null) {
          LOG.error("Tuple value was null");
        }
        else {
          Iterator<String> values = alist.iterator();
          while (values.hasNext()) {
            lstr += "\"" + values.next() + "\"";
          }
        }
        LOG.debug(lstr);
      }
    }
  }
}
