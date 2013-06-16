/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.Sink;
import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.InvertIndexArray;
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
public class InvertIndexArrayBenchmark
{
  private static Logger log = LoggerFactory.getLogger(InvertIndexArrayBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    InvertIndexArray<String,String> oper = new InvertIndexArray<String,String>();
    TestSink indexSink = new TestSink();
    Sink<HashMap<String, ArrayList<String>>> inSink = oper.data.getSink();
    oper.index.setSink(indexSink);

    HashMap<String, ArrayList<String>> input = new HashMap<String, ArrayList<String>>();
    ArrayList<String> alist = new ArrayList<String>();
    oper.beginWindow(0);

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      alist.clear();
      input.clear();
      alist.add("str");
      alist.add("str1");
      input.put("a", alist);
      input.put("b", alist);
      inSink.put(input);
      alist.clear();
      input.clear();

      alist.add("blah");
      alist.add("str1");
      input.put("c", alist);
      inSink.put(input);
    }

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, indexSink.collectedTuples.size());
    for (Object o: indexSink.collectedTuples) {
      HashMap<String, ArrayList<String>> output = (HashMap<String, ArrayList<String>>)o;
      for (Map.Entry<String, ArrayList<String>> e: output.entrySet()) {
        String key = e.getKey();
        alist = e.getValue();
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
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 3));
  }
}
