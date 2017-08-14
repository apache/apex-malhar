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
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 *
 * Functional tests for {@link ChangeAlert}. <p>
 * @deprecated
 */
@Deprecated
public class ChangeAlertTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeAlertTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlert<Integer>());
    testNodeProcessingSchema(new ChangeAlert<Double>());
    testNodeProcessingSchema(new ChangeAlert<Float>());
    testNodeProcessingSchema(new ChangeAlert<Short>());
    testNodeProcessingSchema(new ChangeAlert<Long>());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public <V extends Number> void testNodeProcessingSchema(ChangeAlert<V> oper)
  {
    CollectorTestSink alertSink = new CollectorTestSink();

    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.beginWindow(0);
    oper.data.process(oper.getValue(10));
    oper.data.process(oper.getValue(12)); // alert
    oper.data.process(oper.getValue(12));
    oper.data.process(oper.getValue(18)); // alert
    oper.data.process(oper.getValue(0));  // alert
    oper.data.process(oper.getValue(20)); // this will not alert
    oper.data.process(oper.getValue(30)); // alert

    oper.endWindow();

    // One for a, Two for b
    Assert.assertEquals("number emitted tuples", 4, alertSink.collectedTuples.size());

    double aval = 0;
    log.debug("\nLogging tuples");
    for (Object o: alertSink.collectedTuples) {
      KeyValPair<Number, Double> map = (KeyValPair<Number, Double>)o;
      log.debug(o.toString());
      aval += map.getValue().doubleValue();
    }
    Assert.assertEquals("change in a", 220.0, aval,0);
  }
}
