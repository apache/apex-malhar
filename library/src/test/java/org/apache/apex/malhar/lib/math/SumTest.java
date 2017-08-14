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


/**
 * Functional tests for {@link org.apache.apex.malhar.lib.math.Sum}.
 */
public class SumTest
{
  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeTypeProcessing()
  {
    Sum<Double> doper = new Sum<Double>();
    Sum<Float> foper = new Sum<Float>();
    Sum<Integer> ioper = new Sum<Integer>();
    Sum<Long> loper = new Sum<Long>();
    Sum<Short> soper = new Sum<Short>();
    doper.setType(Double.class);
    foper.setType(Float.class);
    ioper.setType(Integer.class);
    loper.setType(Long.class);
    soper.setType(Short.class);

    testNodeSchemaProcessing(doper);
    testNodeSchemaProcessing(foper);
    testNodeSchemaProcessing(ioper);
    testNodeSchemaProcessing(loper);
    testNodeSchemaProcessing(soper);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testNodeSchemaProcessing(Sum oper)
  {
    CollectorTestSink sumSink = new CollectorTestSink();
    oper.sum.setSink(sumSink);

    oper.beginWindow(0); //

    Double a = 2.0;
    Double b = 20.0;
    Double c = 1000.0;

    oper.data.process(a);
    oper.data.process(b);
    oper.data.process(c);

    a = 1.0;
    oper.data.process(a);
    a = 10.0;
    oper.data.process(a);
    b = 5.0;
    oper.data.process(b);

    b = 12.0;
    oper.data.process(b);
    c = 22.0;
    oper.data.process(c);
    c = 14.0;
    oper.data.process(c);

    a = 46.0;
    oper.data.process(a);
    b = 2.0;
    oper.data.process(b);
    a = 23.0;
    oper.data.process(a);

    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 1,
        sumSink.collectedTuples.size());
    for (Object o : sumSink.collectedTuples) { // sum is 1157

      Double val = ((Number)o).doubleValue();
      Assert.assertEquals("emitted sum value was was ", new Double(1157.0), val);
    }
  }
}
