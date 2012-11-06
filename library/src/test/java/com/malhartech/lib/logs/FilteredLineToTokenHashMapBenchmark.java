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
 * Functional tests for {@link com.malhartech.lib.logs.FilteredLineToTokenHashMap}<p>
 *
 */
public class FilteredLineToTokenHashMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FilteredLineToTokenHashMapBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeProcessing()
  {

    FilteredLineToTokenHashMap oper = new FilteredLineToTokenHashMap();
    TestHashSink tokenSink = new TestHashSink();

    oper.setSplitBy(";");
    oper.setSplitTokenBy(",");
    oper.tokens.setSink(tokenSink);
    ArrayList<String> filters = new ArrayList<String>();
    filters.add("a");
    filters.add("c");
    oper.setSubTokenFilters(filters);
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));

    oper.beginWindow(0); //
    String input1 = "a,2,3;b,1,2;c,4,5,6";
    String input2 = "d";
    String input3 = "";
    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow(); //
    Assert.assertEquals("number emitted tuples", 2, tokenSink.map.size());
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 3));
  }
}
