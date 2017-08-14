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

import org.apache.apex.malhar.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.math.Min}<p>
 *
 */
public class MinTest
{
  /**
   * Test oper logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeSchemaProcessing()
  {
    Min<Double> oper = new Min<Double>();
    CountAndLastTupleTestSink minSink = new CountAndLastTupleTestSink();
    oper.min.setSink(minSink);

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

    Assert.assertEquals("number emitted tuples", 1, minSink.count);
    Assert.assertEquals("emitted high value was ", 1.0, minSink.tuple);
  }
}
