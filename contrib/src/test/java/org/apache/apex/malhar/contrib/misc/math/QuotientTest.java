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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Sink;

/**
 * @deprecated
 * Functional tests for {@link Quotient}
 */
@Deprecated
public class QuotientTest
{

  class TestSink implements Sink<Object>
  {
    List<Object> collectedTuples = new ArrayList<Object>();

    @Override
    public void put(Object payload)
    {
      collectedTuples.add(payload);
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  /**
   * Test oper logic emits correct results.
   */
  @Test
  public void testNodeSchemaProcessing()
  {
    Quotient<Double> oper = new Quotient<Double>();
    TestSink quotientSink = new TestSink();
    oper.quotient.setSink(quotientSink);

    oper.setMult_by(2);

    oper.beginWindow(0); //
    Double a = 30.0;
    Double b = 20.0;
    Double c = 100.0;
    oper.denominator.process(a);
    oper.denominator.process(b);
    oper.denominator.process(c);

    a = 5.0;
    oper.numerator.process(a);
    a = 1.0;
    oper.numerator.process(a);
    b = 44.0;
    oper.numerator.process(b);

    b = 10.0;
    oper.numerator.process(b);
    c = 22.0;
    oper.numerator.process(c);
    c = 18.0;
    oper.numerator.process(c);

    a = 0.5;
    oper.numerator.process(a);
    b = 41.5;
    oper.numerator.process(b);
    a = 8.0;
    oper.numerator.process(a);
    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 1,
        quotientSink.collectedTuples.size());
    for (Object o : quotientSink.collectedTuples) { // sum is 1157
      Double val = (Double)o;
      Assert.assertEquals("emitted quotient value was ", new Double(2.0), val);
    }
  }
}
