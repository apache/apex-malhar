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

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * Functional tests for {@link org.apache.apex.malhar.lib.math.MarginKeyVal}.
 */
public class MarginKeyValTest
{
  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MarginKeyVal<String, Integer>());
    testNodeProcessingSchema(new MarginKeyVal<String, Double>());
    testNodeProcessingSchema(new MarginKeyVal<String, Float>());
    testNodeProcessingSchema(new MarginKeyVal<String, Short>());
    testNodeProcessingSchema(new MarginKeyVal<String, Long>());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testNodeProcessingSchema(MarginKeyVal oper)
  {
    CollectorTestSink marginSink = new CollectorTestSink();

    oper.margin.setSink(marginSink);

    oper.beginWindow(0);
    oper.numerator.process(new KeyValPair("a", 2));
    oper.numerator.process(new KeyValPair("b", 20));
    oper.numerator.process(new KeyValPair("c", 1000));

    oper.denominator.process(new KeyValPair("a", 2));
    oper.denominator.process(new KeyValPair("b", 40));
    oper.denominator.process(new KeyValPair("c", 500));
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3,
        marginSink.collectedTuples.size());
    for (int i = 0; i < marginSink.collectedTuples.size(); i++) {
      if ("a".equals(((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getKey())) {
        Assert.assertEquals("emitted value for 'a' was ", 0d,
            ((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getValue().doubleValue(), 0);
      }
      if ("b".equals(((KeyValPair<String, Number>)marginSink.collectedTuples
          .get(i)).getKey())) {
        Assert.assertEquals("emitted value for 'b' was ", 0.5,
            ((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getValue().doubleValue(), 0);
      }
      if ("c".equals(((KeyValPair<String, Number>)marginSink.collectedTuples
          .get(i)).getKey())) {
        Assert.assertEquals("emitted value for 'c' was ", (double)-1,
            ((KeyValPair<String, Number>)marginSink.collectedTuples.get(i)).getValue().doubleValue(), 0);
      }
    }
  }
}
