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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.HashTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.logs.LineToTokenHashMap}.
 */
public class LineToTokenHashMapTest
{
  /**
   * Test oper logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing()
  {

    LineToTokenHashMap oper = new LineToTokenHashMap();
    HashTestSink tokenSink = new HashTestSink();

    oper.setSplitBy(";");
    oper.setSplitTokenBy(",");
    oper.tokens.setSink(tokenSink);

    oper.beginWindow(0); //
    String input1 = "a,2,3;b,1,2;c,4,5,6";
    String input2 = "d";
    String input3 = "";
    int numTuples = 1000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow(); //
    Assert.assertEquals("number emitted tuples", 2, tokenSink.map.size());
    HashMap<HashMap<String, ArrayList<String>>, Object> smap = tokenSink.map;
    for (Map.Entry<HashMap<String, ArrayList<String>>, Object> e : smap
        .entrySet()) {
      for (Map.Entry<String, ArrayList<String>> l : e.getKey().entrySet()) {
        String key = l.getKey();
        ArrayList<String> list = l.getValue();
        if (key.equals("a")) {
          Assert
              .assertEquals("number emitted values for \"a\"", 2, list.size());
          Assert.assertEquals("first value for \"a\"", "2", list.get(0));
          Assert.assertEquals("second value for \"a\"", "3", list.get(1));
        } else if (key.equals("b")) {
          Assert
              .assertEquals("number emitted values for \"b\"", 2, list.size());
          Assert.assertEquals("first value for \"b\"", "1", list.get(0));
          Assert.assertEquals("second value for \"b\"", "2", list.get(1));
        } else if (key.equals("c")) {
          Assert
              .assertEquals("number emitted values for \"c\"", 3, list.size());
          Assert.assertEquals("first value for \"c\"", "4", list.get(0));
          Assert.assertEquals("second value for \"c\"", "5", list.get(1));
          Assert.assertEquals("second value for \"c\"", "6", list.get(2));
        } else if (key.equals("d")) {
          Assert
              .assertEquals("number emitted values for \"d\"", 0, list.size());
        }
      }
    }
  }
}
