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
package org.apache.apex.malhar.contrib.misc.math;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * @deprecated
 * Functional tests for {@link SumCountMap}.
 */
@Deprecated
public class SumCountMapTest
{
  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    testNodeSchemaProcessing(true, true);
    testNodeSchemaProcessing(true, false);
    testNodeSchemaProcessing(false, true);
    testNodeSchemaProcessing(false, false);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testNodeSchemaProcessing(boolean sum, boolean count)
  {
    SumCountMap<String, Double> oper = new SumCountMap<String, Double>();
    oper.setType(Double.class);
    CollectorTestSink sumSink = new CollectorTestSink();
    CollectorTestSink countSink = new CollectorTestSink();
    if (sum) {
      oper.sum.setSink(sumSink);
    }
    if (count) {
      oper.count.setSink(countSink);
    }

    oper.beginWindow(0); //

    HashMap<String, Double> input = new HashMap<String, Double>();

    input.put("a", 2.0);
    input.put("b", 20.0);
    input.put("c", 1000.0);
    oper.data.process(input);
    input.clear();
    input.put("a", 1.0);
    oper.data.process(input);
    input.clear();
    input.put("a", 10.0);
    input.put("b", 5.0);
    oper.data.process(input);
    input.clear();
    input.put("d", 55.0);
    input.put("b", 12.0);
    oper.data.process(input);
    input.clear();
    input.put("d", 22.0);
    oper.data.process(input);
    input.clear();
    input.put("d", 14.2);
    oper.data.process(input);
    input.clear();

    // Mix integers and doubles
    HashMap<String, Double> inputi = new HashMap<String, Double>();
    inputi.put("d", 46.0);
    inputi.put("e", 2.0);
    oper.data.process(inputi);
    inputi.clear();
    inputi.put("a", 23.0);
    inputi.put("d", 4.0);
    oper.data.process(inputi);
    inputi.clear();

    oper.endWindow(); //

    if (sum) {
      // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
      Assert.assertEquals("number emitted tuples", 1, sumSink.collectedTuples.size());

      for (Object o : sumSink.collectedTuples) {
        HashMap<String, Object> output = (HashMap<String, Object>)o;
        for (Map.Entry<String, Object> e : output.entrySet()) {
          Double val = (Double)e.getValue();
          if (e.getKey().equals("a")) {
            Assert.assertEquals("emitted value for 'a' was ", new Double(36),
                val);
          } else if (e.getKey().equals("b")) {
            Assert.assertEquals("emitted tuple for 'b' was ", new Double(37),
                val);
          } else if (e.getKey().equals("c")) {
            Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000),
                val);
          } else if (e.getKey().equals("d")) {
            Assert.assertEquals("emitted tuple for 'd' was ",
                new Double(141.2), val);
          } else if (e.getKey().equals("e")) {
            Assert.assertEquals("emitted tuple for 'e' was ", new Double(2),
                val);
          }
        }
      }
    }
    if (count) {
      // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
      Assert.assertEquals("number emitted tuples", 1, countSink.collectedTuples.size());
      for (Object o : countSink.collectedTuples) {
        HashMap<String, Object> output = (HashMap<String, Object>)o;
        for (Map.Entry<String, Object> e : output.entrySet()) {
          Integer val = (Integer)e.getValue();
          if (e.getKey().equals("a")) {
            Assert
                .assertEquals("emitted value for 'a' was ", 4, val.intValue());
          } else if (e.getKey().equals("b")) {
            Assert
                .assertEquals("emitted tuple for 'b' was ", 3, val.intValue());
          } else if (e.getKey().equals("c")) {
            Assert
                .assertEquals("emitted tuple for 'c' was ", 1, val.intValue());
          } else if (e.getKey().equals("d")) {
            Assert
                .assertEquals("emitted tuple for 'd' was ", 5, val.intValue());
          } else if (e.getKey().equals("e")) {
            Assert
                .assertEquals("emitted tuple for 'e' was ", 1, val.intValue());
          }
        }
      }
    }
  }
}
