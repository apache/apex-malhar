/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.InvertIndex;
import com.malhartech.api.Sink;
import com.malhartech.engine.TestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.InvertIndex} <p>
 *
 */
public class InvertIndexBenchmark
{
  private static Logger log = LoggerFactory.getLogger(InvertIndexBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    InvertIndex<String,String> oper = new InvertIndex<String,String>();
    TestSink indexSink = new TestSink();

    Sink inSink = oper.data.getSink();
    oper.index.setSink(indexSink);

    HashMap<String, String> input = new HashMap<String, String>();
    oper.beginWindow(0);

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a", "str");
      input.put("b", "str");
      inSink.put(input);

      input.clear();
      input.put("a", "str1");
      input.put("b", "str1");
      inSink.put(input);

      input.clear();
      input.put("c", "blah");
      inSink.put(input);

      input.clear();
      input.put("c", "str1");
      inSink.put(input);
    }

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, indexSink.collectedTuples.size());
    for (Object o: indexSink.collectedTuples) {
      HashMap<String, ArrayList<String>> output = (HashMap<String, ArrayList<String>>)o;
      for (Map.Entry<String, ArrayList<String>> e: output.entrySet()) {
        String key = e.getKey();
        ArrayList<String> alist = e.getValue();
        if (key.equals("str1")) {
          Assert.assertEquals("Index for \"str1\" contains \"a\"", true, alist.contains("a"));
          Assert.assertEquals("Index for \"str1\" contains \"b\"", true, alist.contains("b"));
          Assert.assertEquals("Index for \"str1\" contains \"c\"", true, alist.contains("c"));
        }
        else if (key.equals("str")) {
          Assert.assertEquals("Index for \"str1\" contains \"a\"", true, alist.contains("a"));
          Assert.assertEquals("Index for \"str1\" contains \"b\"", true, alist.contains("b"));
          Assert.assertEquals("Index for \"str1\" contains \"c\"", false, alist.contains("c"));
        }
        else if (key.equals("blah")) {
          Assert.assertEquals("Index for \"str1\" contains \"a\"", false, alist.contains("a"));
          Assert.assertEquals("Index for \"str1\" contains \"b\"", false, alist.contains("b"));
          Assert.assertEquals("Index for \"str1\" contains \"c\"", true, alist.contains("c"));
        }
      }
    }
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 6));
  }
}
