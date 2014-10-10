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
package com.datatorrent.lib.math;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.CompareExceptStringMap}<p>
 *
 */
public class CompareExceptStringMapTest
{
  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessingSchema()
  {
    CompareExceptStringMap<String> oper = new CompareExceptStringMap<String>();
    CountAndLastTupleTestSink compareSink = new CountAndLastTupleTestSink();
    CountAndLastTupleTestSink exceptSink = new CountAndLastTupleTestSink();
    oper.compare.setSink(compareSink);
    oper.except.setSink(exceptSink);

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
    input.put("b", "32");
    input.put("c", "23");
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, exceptSink.count);
    for (Map.Entry<String, String> e: ((HashMap<String, String>)exceptSink.tuple).entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", "2", e.getValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", "20", e.getValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", "1000", e.getValue());
      }
    }

    Assert.assertEquals("number emitted tuples", 1, compareSink.count);
    for (Map.Entry<String, String> e: ((HashMap<String, String>)compareSink.tuple).entrySet()) {
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", "3", e.getValue());
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", "32", e.getValue());
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", "23", e.getValue());
      }
    }
  }
}
