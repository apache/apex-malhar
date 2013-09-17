/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.lang.mutable.MutableDouble;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.algo.TopNUniqueTest;
import com.datatorrent.lib.logs.MultiWindowDimensionAggregation;
import com.datatorrent.lib.logs.MultiWindowDimensionAggregation.AggregateOperation;
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

      
      oper.setWindowSize(2);
      List<int []> dimensionArrayList = new ArrayList<int[]>();
      int[] dimensionArray = {0,1};
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
      Map<String, Map<String,MutableDouble>> data = new HashMap<String, Map<String,MutableDouble>>();
      Map<String, MutableDouble> input = new HashMap<String, MutableDouble>();

      input.put("0",new MutableDouble(9));
      input.put("1",new MutableDouble(9));
      input.put("2",new MutableDouble(9));
      data.put("m|20130823131512|0:abc|1:ff",input);
      data.put("m|20130823131512|0:abc",input);
      oper.data.process(data);
      input.clear();
      data.clear();

      input.put("0",new MutableDouble(19));
      input.put("1",new MutableDouble(19));
      input.put("2",new MutableDouble(19));
      data.put("m|20130823131512|0:abc|1:ie",input);
      Map<String, MutableDouble> input_new = new HashMap<String, MutableDouble>();
      input_new.put("0",new MutableDouble(19));
      input_new.put("1",new MutableDouble(19));
      input_new.put("2",new MutableDouble(19));
      data.put("m|20130823131512|0:def|1:ie",input_new);
      oper.data.process(data);
      oper.endWindow();

      Assert.assertEquals("number emitted tuples", 4, sortSink.collectedTuples.size());
      for (Object o : sortSink.collectedTuples) {
        log.debug(o.toString());
        //System.out.println(o.toString());
      }
      sortSink.clear();
      //System.out.println("end of window");
      
      oper.beginWindow(1);
      input.clear();
      data.clear();
      input.put("0",new MutableDouble(9));
      input.put("1",new MutableDouble(9));
      input.put("2",new MutableDouble(9));
      data.put("m|20130823131513|0:def|1:ff",input);
      oper.data.process(data);

      input.clear();
      data.clear();

      input.put("0",new MutableDouble(9));
      input.put("1",new MutableDouble(9));
      input.put("2",new MutableDouble(9));
      data.put("m|20130823131513|0:abc|1:ie",input);
      oper.data.process(data);
      oper.endWindow();

    Assert.assertEquals("number emitted tuples", 5, sortSink.collectedTuples.size());
      for (Object o : sortSink.collectedTuples) {
        log.debug(o.toString());
        //System.out.println(o.toString());
      }
      sortSink.clear();
//      System.out.println("end of window");
      
      oper.beginWindow(2);
      input.clear();
      data.clear();
      input.put("0",new MutableDouble(19));
      input.put("1",new MutableDouble(19));
      input.put("2",new MutableDouble(19));
      data.put("m|20130823131514|0:def|1:ff",input);
      oper.data.process(data);

      input.clear();
      data.clear();

      input.put("0",new MutableDouble(19));
      input.put("1",new MutableDouble(19));
      input.put("2",new MutableDouble(19));
      data.put("m|20130823131514|0:abc|1:ie",input);
      oper.data.process(data);
      oper.endWindow();

      
      Assert.assertEquals("number emitted tuples", 2,	sortSink.collectedTuples.size());
      for (Object o : sortSink.collectedTuples) {
        log.debug(o.toString());
        //System.out.println(o.toString());
      }
      log.debug("Done testing round\n");
    }
}
