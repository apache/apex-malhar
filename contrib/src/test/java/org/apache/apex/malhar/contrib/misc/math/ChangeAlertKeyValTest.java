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
 * Functional tests for {@link ChangeAlertKeyVal}.
 * <p>
 * @deprecated
 */
@Deprecated
public class ChangeAlertKeyValTest
{
  private static Logger log = LoggerFactory
      .getLogger(ChangeAlertKeyValTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Integer>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Double>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Float>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Short>());
    testNodeProcessingSchema(new ChangeAlertKeyVal<String, Long>());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public <V extends Number> void testNodeProcessingSchema(
      ChangeAlertKeyVal<String, V> oper)
  {
    CollectorTestSink alertSink = new CollectorTestSink();

    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.beginWindow(0);
    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(200)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(10)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(100)));

    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(203)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(12)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(101)));

    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(210)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(12)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(102)));

    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(231)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(18)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(103)));
    oper.endWindow();

    // One for a, Two for b
    Assert.assertEquals("number emitted tuples", 3,
        alertSink.collectedTuples.size());

    double aval = 0;
    double bval = 0;
    log.debug("\nLogging tuples");
    for (Object o : alertSink.collectedTuples) {
      KeyValPair<String, KeyValPair<Number, Double>> map = (KeyValPair<String, KeyValPair<Number, Double>>)o;

      log.debug(o.toString());
      if (map.getKey().equals("a")) {
        KeyValPair<Number, Double> vmap = map.getValue();
        if (vmap != null) {
          aval += vmap.getValue().doubleValue();
        }
      } else {
        KeyValPair<Number, Double> vmap = map.getValue();
        if (vmap != null) {
          bval += vmap.getValue().doubleValue();
        }
      }
    }
    Assert.assertEquals("change in a", 10.0, aval,0);
    Assert.assertEquals("change in a", 70.0, bval,0);
  }
}
