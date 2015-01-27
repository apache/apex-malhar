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

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.UniqueValueKeyVal}<p>
 *
 */
public class UniqueValueMapTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    UniqueValueMap<String> oper = new UniqueValueMap<String>();
    CollectorTestSink<HashMap<String, Integer>> sink = new CollectorTestSink<HashMap<String, Integer>>();
    TestUtils.setSink(oper.count, sink);

    HashMap<String, Integer> h1 = new HashMap<String, Integer>();
    h1.put("a", 1);
    h1.put("b", 1);

    HashMap<String,Integer> h2 = new HashMap<String, Integer>();
    h2.put("a", 2);
    h2.put("c", 5);
    h2.put("e", 5);

    HashMap<String,Integer> h3 = new HashMap<String, Integer>();
    h3.put("d", 2);
    h3.put("e", 2);

    int numTuples = 10000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(h1);
      if (i % 2 == 0) {
        oper.data.process(h2);
      }
      if (i % 3 == 0) {
        oper.data.process(h3);
      }

    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
    HashMap<String, Integer> e = sink.collectedTuples.get(0);
    Assert.assertEquals("emitted value for 'a' was ", 2, e.get("a").intValue());
    Assert.assertEquals("emitted value for 'b' was ", 1, e.get("b").intValue());
    Assert.assertEquals("emitted value for 'c' was ", 1, e.get("c").intValue());
    Assert.assertEquals("emitted value for 'd' was ", 1, e.get("d").intValue());
    Assert.assertEquals("emitted value for 'e' was ", 2, e.get("e").intValue());
  }
}
