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
 * Functional tests for {@link org.apache.apex.malhar.lib.math.SumKeyVal}.
 */
public class SumKeyValTest
{
  /**
   * Test operator logic emits correct results.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testNodeProcessing()
  {
    SumKeyVal<String, Double> oper = new SumKeyVal<String, Double>();
    oper.setType(Double.class);
    CollectorTestSink sumSink = new CollectorTestSink();
    oper.sum.setSink(sumSink);

    oper.beginWindow(0); //

    oper.data.process(new KeyValPair("a", 2.0));
    oper.data.process(new KeyValPair("b", 20.0));
    oper.data.process(new KeyValPair("c", 1000.0));
    oper.data.process(new KeyValPair("a", 1.0));
    oper.data.process(new KeyValPair("a", 10.0));
    oper.data.process(new KeyValPair("b", 5.0));
    oper.data.process(new KeyValPair("d", 55.0));
    oper.data.process(new KeyValPair("b", 12.0));
    oper.data.process(new KeyValPair("d", 22.0));
    oper.data.process(new KeyValPair("d", 14.2));
    oper.data.process(new KeyValPair("d", 46.0));
    oper.data.process(new KeyValPair("e", 2.0));
    oper.data.process(new KeyValPair("a", 23.0));
    oper.data.process(new KeyValPair("d", 4.0));

    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 5, sumSink.collectedTuples.size());
    for (Object o: sumSink.collectedTuples) {
      KeyValPair<String, Double> e = (KeyValPair<String, Double>)o;
      Double val = (Double)e.getValue();
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Double(36), val);
      } else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", new Double(37), val);
      } else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000), val);
      } else if (e.getKey().equals("d")) {
        Assert.assertEquals("emitted tuple for 'd' was ", new Double(141.2), val);
      } else if (e.getKey().equals("e")) {
        Assert.assertEquals("emitted tuple for 'e' was ", new Double(2), val);
      }
    }
  }
}
