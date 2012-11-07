/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.dag.TestHashSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.logs.FilteredLineTokenizerKeyVal}<p>
 *
 */
public class FilteredLineTokenizerKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(FilteredLineTokenizerKeyValTest.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeProcessing()
  {

    FilteredLineTokenizerKeyVal oper = new FilteredLineTokenizerKeyVal();
    TestHashSink tokenSink = new TestHashSink();

    oper.setSplitBy(",");
    oper.setSplitTokenBy("=");
    oper.tokens.setSink(tokenSink);
    ArrayList<String> filters = new ArrayList<String>();
    filters.add("a");
    filters.add("c");
    oper.setSubTokenFilters(filters);

    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null, null));
    oper.beginWindow(0); //

    String input1 = "a=2,b=3,c=4";
    String input2 = "d=2";
    String input3 = "";
    int numTuples = 1000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow(); //
    Assert.assertEquals("number emitted tuples", 2, tokenSink.map.size());
    HashMap<Object, Object> smap = (HashMap<Object, Object>)tokenSink.map;
    for (Map.Entry<Object, Object> e: smap.entrySet()) {
      HashMap<String, String> kmap = (HashMap<String, String>)e.getKey();
      for (Map.Entry<String, String> o: kmap.entrySet()) {
        String key = o.getKey();
        String val = o.getValue();
        Assert.assertTrue(!key.equals("b"));
        Assert.assertTrue(!key.equals("d"));
        if (key.equals("a")) {
          Assert.assertEquals("value of \"a\"", "2", val);
        }
        else if (key.equals("c")) {
          Assert.assertEquals("value of \"c\"", "4", val);
        }
      }
    }
  }
}
