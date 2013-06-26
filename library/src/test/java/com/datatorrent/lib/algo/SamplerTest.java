/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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

import com.datatorrent.lib.algo.Sampler;
import com.datatorrent.lib.testbench.CountTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.Sampler}<p>
 *
 */
public class SamplerTest
{
  private static Logger log = LoggerFactory.getLogger(SamplerTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    Sampler<String> oper = new Sampler<String>();
    CountTestSink sink = new CountTestSink<String>();
    oper.sample.setSink(sink);
    oper.setPassrate(10);
    oper.setTotalrate(100);

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
