/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.logs;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.logs.FilteredLineToTokenArrayList;
import com.datatorrent.lib.testbench.ArrayListTestSink;
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
 * Functional tests for {@link com.datatorrent.lib.logs.LineToTokenArrayList}<p>
 *
 */
public class FilteredLineToTokenArrayListBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FilteredLineToTokenArrayListBenchmark.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {

    FilteredLineToTokenArrayList oper = new FilteredLineToTokenArrayList();
    ArrayListTestSink tokenSink = new ArrayListTestSink();
    ArrayListTestSink stokenSink = new ArrayListTestSink();

    oper.setSplitBy(";");
    oper.setSplitTokenBy(",");
    oper.tokens.setSink(tokenSink);
    oper.splittokens.setSink(stokenSink);
    String [] filters = new String[2];
    filters[0] = "a";
    filters[1] = "c";
    oper.setFilterBy(filters);
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
    Assert.assertEquals("number emitted tuples", 4, tokenSink.map.size());
    Assert.assertEquals("number emitted tuples", 2, stokenSink.map.size());
    Assert.assertEquals("number emitted tuples", numTuples, tokenSink.getCount("a,2,3"));
    Assert.assertEquals("number emitted tuples", numTuples, tokenSink.getCount("b,1,2"));
    Assert.assertEquals("number emitted tuples", numTuples, tokenSink.getCount("c,4,5,6"));
    Assert.assertEquals("number emitted tuples", numTuples, tokenSink.getCount("d"));
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 3));
  }
}
