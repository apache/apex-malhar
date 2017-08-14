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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 *
 * Functional tests for {@link Change}.
 * <p>
 * @deprecated
 */
@Deprecated
public class ChangeTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Change<Integer>());
    testNodeProcessingSchema(new Change<Double>());
    testNodeProcessingSchema(new Change<Float>());
    testNodeProcessingSchema(new Change<Short>());
    testNodeProcessingSchema(new Change<Long>());
  }

  /**
   *
   * @param oper  Data value for comparison.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public <V extends Number> void testNodeProcessingSchema(Change<V> oper)
  {
    CollectorTestSink changeSink = new CollectorTestSink();
    CollectorTestSink percentSink = new CollectorTestSink();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    oper.base.process(oper.getValue(10));
    oper.data.process(oper.getValue(5));
    oper.data.process(oper.getValue(15));
    oper.data.process(oper.getValue(20));
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3,
        changeSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 3,
        percentSink.collectedTuples.size());

    log.debug("\nLogging tuples");
    for (Object o : changeSink.collectedTuples) {
      log.debug(String.format("change %s", o));
    }
    for (Object o : percentSink.collectedTuples) {
      log.debug(String.format("percent change %s", o));
    }
  }
}
