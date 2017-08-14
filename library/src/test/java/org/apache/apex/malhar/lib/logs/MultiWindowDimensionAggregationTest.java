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
package org.apache.apex.malhar.lib.logs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.logs.MultiWindowDimensionAggregation.AggregateOperation;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 * <p>
 * MultiWindowDimensionAggregationTest class.
 * </p>
 *
 */
public class MultiWindowDimensionAggregationTest
{

  private static Logger logger = LoggerFactory.getLogger(MultiWindowDimensionAggregationTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MultiWindowDimensionAggregation());

  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testNodeProcessingSchema(MultiWindowDimensionAggregation oper)
  {

    oper.setWindowSize(3);
    List<int[]> dimensionArrayList = new ArrayList<int[]>();
    int[] dimensionArray = {0, 1};
    int[] dimensionArray_2 = {0};
    dimensionArrayList.add(dimensionArray);
    dimensionArrayList.add(dimensionArray_2);
    oper.setDimensionArray(dimensionArrayList);

    oper.setTimeBucket("m");
    oper.setDimensionKeyVal("0");

    oper.setOperationType(AggregateOperation.AVERAGE);
    oper.setup(null);
    CollectorTestSink sortSink = new CollectorTestSink();
    oper.output.setSink(sortSink);

    oper.beginWindow(0);
    Map<String, Map<String, Number>> data_0 = new HashMap<String, Map<String, Number>>();
    Map<String, Number> input_0 = new HashMap<String, Number>();

    input_0.put("0", new MutableDouble(9));
    input_0.put("1", new MutableDouble(9));
    input_0.put("2", new MutableDouble(9));
    data_0.put("m|20130823131512|0:abc|1:ff", input_0);
    data_0.put("m|20130823131512|0:abc", input_0);
    data_0.put("m|20130823131512|0:abc|1:ie", input_0);
    Map<String, Number> input_new = new HashMap<String, Number>();
    input_new.put("0", new MutableDouble(19));
    input_new.put("1", new MutableDouble(19));
    input_new.put("2", new MutableDouble(19));
    data_0.put("m|20130823131512|0:def|1:ie", input_new);
    oper.data.process(data_0);
    oper.endWindow();

    Map<String, Map<String, Number>> data_1 = new HashMap<String, Map<String, Number>>();
    Map<String, Number> input_1 = new HashMap<String, Number>();
    oper.beginWindow(1);

    input_1.put("0", new MutableDouble(9));
    input_1.put("1", new MutableDouble(9));
    input_1.put("2", new MutableDouble(9));
    data_1.put("m|20130823131513|0:def|1:ff", input_1);
    data_1.put("m|20130823131513|0:abc|1:ie", input_1);
    oper.data.process(data_1);
    oper.endWindow();

    Map<String, Map<String, Number>> data_2 = new HashMap<String, Map<String, Number>>();
    Map<String, Number> input_2 = new HashMap<String, Number>();
    oper.beginWindow(2);

    input_2.put("0", new MutableDouble(19));
    input_2.put("1", new MutableDouble(19));
    input_2.put("2", new MutableDouble(19));
    data_2.put("m|20130823131514|0:def|1:ff", input_2);
    data_2.put("m|20130823131514|0:abc|1:ie", input_2);
    oper.data.process(data_2);
    oper.endWindow();

    Map<String, Map<String, Number>> data_3 = new HashMap<String, Map<String, Number>>();
    Map<String, Number> input_3 = new HashMap<String, Number>();
    oper.beginWindow(3);
    input_3.put("0", new MutableDouble(19));
    input_3.put("1", new MutableDouble(19));
    input_3.put("2", new MutableDouble(19));
    data_3.put("m|20130823131514|0:def|1:ff", input_3);
    data_3.put("m|20130823131514|0:abc|1:ie", input_3);
    oper.data.process(data_3);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 16, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      logger.debug(o.toString());
    }
    logger.debug("Done testing round\n");
  }
}
