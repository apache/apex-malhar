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
package org.apache.apex.malhar.lib.math;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.math.MarginMap}
 * <p>
 *
 */
public class MarginMapTest
{
  private static Logger LOG = LoggerFactory.getLogger(MarginMapTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MarginMap<String, Integer>());
    testNodeProcessingSchema(new MarginMap<String, Double>());
    testNodeProcessingSchema(new MarginMap<String, Float>());
    testNodeProcessingSchema(new MarginMap<String, Short>());
    testNodeProcessingSchema(new MarginMap<String, Long>());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testNodeProcessingSchema(MarginMap oper)
  {
    CountAndLastTupleTestSink marginSink = new CountAndLastTupleTestSink();

    oper.margin.setSink(marginSink);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.numerator.process(input);

    input.clear();
    input.put("a", 2);
    input.put("b", 40);
    input.put("c", 500);
    oper.denominator.process(input);

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, marginSink.count);

    HashMap<String, Number> output = (HashMap<String, Number>)marginSink.tuple;
    for (Map.Entry<String, Number> e : output.entrySet()) {
      LOG.debug(String.format("Key, value is %s,%f", e.getKey(), e.getValue().doubleValue()));
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", 0d, e.getValue().doubleValue(), 0);
      } else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", 0.5, e.getValue().doubleValue(), 0);
      } else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", -1.0, e.getValue().doubleValue(), 0);
      } else {
        LOG.debug(String.format("key was %s", e.getKey()));
      }
    }
  }
}
