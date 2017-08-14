/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.logs;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.HashTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.logs.FilteredLineTokenizerKeyVal}<p>
 *
 */
public class FilteredLineTokenizerKeyValTest
{
  /**
   * Test oper logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing()
  {

    FilteredLineTokenizerKeyVal oper = new FilteredLineTokenizerKeyVal();
    HashTestSink tokenSink = new HashTestSink();

    oper.setSplitBy(",");
    oper.setSplitTokenBy("=");
    oper.tokens.setSink(tokenSink);
    String[] filters = new String[2];
    filters[0] = "a";
    filters[1] = "c";
    oper.setFilterBy(filters);

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
    HashMap<Object, Object> smap = tokenSink.map;
    for (Map.Entry<Object, Object> e : smap.entrySet()) {
      HashMap<String, String> kmap = (HashMap<String, String>)e.getKey();
      for (Map.Entry<String, String> o : kmap.entrySet()) {
        String key = o.getKey();
        String val = o.getValue();
        Assert.assertTrue(!key.equals("b"));
        Assert.assertTrue(!key.equals("d"));
        if (key.equals("a")) {
          Assert.assertEquals("value of \"a\"", "2", val);
        } else if (key.equals("c")) {
          Assert.assertEquals("value of \"c\"", "4", val);
        }
      }
    }
  }
}
