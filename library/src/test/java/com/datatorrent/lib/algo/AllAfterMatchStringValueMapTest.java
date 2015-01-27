/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.algo;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * 
 * Functional tests for
 * {@link com.datatorrent.lib.algo.AllAfterMatchStringValueMap}.
 * <p>
 * 
 */
public class AllAfterMatchStringValueMapTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {

    AllAfterMatchStringValueMap<String> oper = new AllAfterMatchStringValueMap<String>();
    CollectorTestSink<HashMap<String, String>> allSink = new CollectorTestSink<HashMap<String, String>>();
    TestUtils.setSink(oper.allafter, allSink);
    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, String> input = new HashMap<String, String>();
    input.put("a", "2");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "3");
    oper.data.process(input);

    input.clear();
    input.put("b", "6");
    oper.data.process(input);

    input.clear();
    input.put("c", "9");
    oper.data.process(input);

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3,
        allSink.collectedTuples.size());
    for (HashMap<String, String> o : allSink.collectedTuples) {
      for (Map.Entry<String, String> e : o.entrySet()) {
        if (e.getKey().equals("a")) {
          Assert.assertEquals("emitted value for 'a' was ", "3", e.getValue());
        } else if (e.getKey().equals("b")) {
          Assert.assertEquals("emitted tuple for 'b' was ", "6", e.getValue());
        } else if (e.getKey().equals("c")) {
          Assert.assertEquals("emitted tuple for 'c' was ", "9", e.getValue());
        }
      }
    }
  }
}
