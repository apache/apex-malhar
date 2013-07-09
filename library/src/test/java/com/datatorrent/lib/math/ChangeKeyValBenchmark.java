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
package com.datatorrent.lib.math;

import com.datatorrent.lib.math.ChangeKeyVal;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.lib.util.KeyValPair;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.ChangeKeyVal}. <p>
 * Current benchmark 20 millions tuples per sec.
 *
 */
public class ChangeKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeKeyValBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeKeyVal<String, Integer>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Double>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Float>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Short>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeKeyVal<String, V> oper)
  {
    CountTestSink changeSink = new CountTestSink<KeyValPair<String, V>>();
    CountTestSink percentSink = new CountTestSink<KeyValPair<String, Double>>();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);

    oper.base.process(new KeyValPair<String, V>("a", oper.getValue(2)));
    oper.base.process(new KeyValPair<String, V>("b", oper.getValue(10)));
    oper.base.process(new KeyValPair<String, V>("c", oper.getValue(100)));

    int numTuples = 1000000;

    for (int i = 0; i < numTuples; i++) {
      oper.data.process(new KeyValPair<String, V>("a", oper.getValue(3)));
      oper.data.process(new KeyValPair<String, V>("b", oper.getValue(2)));
      oper.data.process(new KeyValPair<String, V>("c", oper.getValue(4)));
    }
    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", numTuples * 3, changeSink.getCount());
    Assert.assertEquals("number emitted tuples", numTuples * 3, percentSink.getCount());
    log.debug(String.format("\nBenchmarked %d key,val pairs", numTuples * 3));
  }
}
