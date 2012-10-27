/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.TestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.InvertIndex} <p>
 *
 */
public class InvertIndexArrayTest
{
  private static Logger LOG = LoggerFactory.getLogger(InvertIndexArray.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    InvertIndexArray oper = new InvertIndexArray();

    TestSink indexSink = new TestSink();

    Sink inSink = oper.data.getSink();
    oper.index.setSink(indexSink);
    oper.setup(new OperatorConfiguration());

    oper.beginWindow();

    HashMap<String, ArrayList> input = new HashMap<String, ArrayList>();
    ArrayList<String> alist = new ArrayList<String>();
    alist.add("str");
    alist.add("str1");
    input.put("a", alist);
    input.put("b", alist);
    inSink.process(input);

    alist = new ArrayList<String>();
    input = new HashMap<String, ArrayList>();
    alist.add("blah");
    alist.add("str1");
    input.put("c", alist);
    inSink.process(input);

    oper.endWindow();
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
