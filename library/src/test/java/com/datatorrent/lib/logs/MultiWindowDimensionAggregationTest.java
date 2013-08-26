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
package com.datatorrent.lib.logs;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.algo.TopNUniqueTest;
import com.datatorrent.lib.logs.MultiWindowDimensionAggregation;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * <p>MultiWindowDimensionAggregationTest class.</p>
 *
 */
public class MultiWindowDimensionAggregationTest {

  private static Logger log = LoggerFactory.getLogger(TopNUniqueTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
    public void testNodeProcessing() throws Exception {
      testNodeProcessingSchema(new MultiWindowDimensionAggregation());

    }

  @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testNodeProcessingSchema(MultiWindowDimensionAggregation oper) {

      oper.setup(null);
      oper.setWindowSize(2);
      int[] dimensionArray = {0,1};
      oper.setDimensionArray(dimensionArray);
      oper.setTimeBucket("m");
      oper.setDimensionKeyVal("0");

      CollectorTestSink sortSink = new CollectorTestSink();
      oper.output.setSink(sortSink);

      oper.beginWindow(0);
      Map<String, Map<String,Number>> data = new HashMap<String, Map<String,Number>>();
      Map<String, Number> input = new HashMap<String, Number>();

      input.put("0",9);
      input.put("1",9);
      input.put("2",9);
      data.put("m|20130823131512|0:abc|1:ff",input);
      oper.data.process(data);
      input.clear();
      data.clear();

      input.put("0",19);
      input.put("1",19);
      input.put("2",19);
      data.put("m|20130823131512|0:abc|1:ie",input);
      oper.data.process(data);
      oper.endWindow();


      oper.beginWindow(1);
      input.clear();
      data.clear();
      input.put("0",9);
      input.put("1",9);
      input.put("2",9);
      data.put("m|20130823131513|0:def|1:ff",input);
      oper.data.process(data);

      input.clear();
      data.clear();

      input.put("0",19);
      input.put("1",19);
      input.put("2",19);
      data.put("m|20130823131513|0:abc|1:ie",input);
      oper.data.process(data);
      oper.endWindow();


      oper.beginWindow(2);
      input.clear();
      data.clear();
      input.put("0",99);
      input.put("1",99);
      input.put("2",99);
      data.put("m|20130823131514|0:def|1:ff",input);
      oper.data.process(data);

      input.clear();
      data.clear();

      input.put("0",19);
      input.put("1",19);
      input.put("2",19);
      data.put("m|20130823131514|0:abc|1:ie",input);
      oper.data.process(data);
      oper.endWindow();

      Assert.assertEquals("number emitted tuples", 7,	sortSink.collectedTuples.size());
      for (Object o : sortSink.collectedTuples) {
        log.debug(o.toString());
      }
      log.debug("Done testing round\n");
    }
}
