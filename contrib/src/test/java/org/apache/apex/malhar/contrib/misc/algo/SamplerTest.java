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
package org.apache.apex.malhar.contrib.misc.algo;

import org.junit.Assert;

import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CountTestSink;

/**
 * @deprecated
 * Functional tests for {@link Sampler}<p>
 *
 */
@Deprecated
public class SamplerTest
{
  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    Sampler<String> oper = new Sampler<String>();
    CountTestSink sink = new CountTestSink<String>();
    oper.sample.setSink(sink);
    oper.setSamplingPercentage(.1);

    String tuple = "a";


    int numTuples = 10000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(tuple);
    }

    oper.endWindow();
    int lowerlimit = 5;
    int upperlimit = 15;
    int actual = (100 * sink.count) / numTuples;

    Assert.assertEquals("number emitted tuples", true, lowerlimit < actual);
    Assert.assertEquals("number emitted tuples", true, upperlimit > actual);
  }
}
