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

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 *
 * Functional tests for {@link ChangeAlertMap}.
 * <p>
 * @deprecated
 */
@Deprecated
public class ChangeAlertMapTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeAlertMapTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlertMap<String, Integer>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Double>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Float>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Short>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Long>());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public <V extends Number> void testNodeProcessingSchema(
      ChangeAlertMap<String, V> oper)
  {
    CollectorTestSink alertSink = new CollectorTestSink();

    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.beginWindow(0);
    HashMap<String, V> input = new HashMap<String, V>();
    input.put("a", oper.getValue(200));
    input.put("b", oper.getValue(10));
    input.put("c", oper.getValue(100));
    oper.data.process(input);

    input.clear();
    input.put("a", oper.getValue(203));
    input.put("b", oper.getValue(12));
    input.put("c", oper.getValue(101));
    oper.data.process(input);

    input.clear();
    input.put("a", oper.getValue(210));
    input.put("b", oper.getValue(12));
    input.put("c", oper.getValue(102));
    oper.data.process(input);

    input.clear();
    input.put("a", oper.getValue(231));
    input.put("b", oper.getValue(18));
    input.put("c", oper.getValue(103));
    oper.data.process(input);
    oper.endWindow();

    // One for a, Two for b
    Assert.assertEquals("number emitted tuples", 3,
        alertSink.collectedTuples.size());

    double aval = 0;
    double bval = 0;
    log.debug("\nLogging tuples");
    for (Object o : alertSink.collectedTuples) {
      HashMap<String, HashMap<Number, Double>> map = (HashMap<String, HashMap<Number, Double>>)o;
      Assert.assertEquals("map size", 1, map.size());
      log.debug(o.toString());
      HashMap<Number, Double> vmap = map.get("a");
      if (vmap != null) {
        aval += vmap.get(231.0).doubleValue();
      }
      vmap = map.get("b");
      if (vmap != null) {
        if (vmap.get(12.0) != null) {
          bval += vmap.get(12.0).doubleValue();
        } else {
          bval += vmap.get(18.0).doubleValue();
        }
      }
    }
    Assert.assertEquals("change in a", 10.0, aval,0);
    Assert.assertEquals("change in a", 70.0, bval,0);
  }
}
