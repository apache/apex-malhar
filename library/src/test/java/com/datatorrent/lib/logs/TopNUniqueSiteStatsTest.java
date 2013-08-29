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
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.algo.TopNUnique;
import com.datatorrent.lib.algo.TopNUniqueTest;
import com.datatorrent.lib.logs.DimensionObject;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * <p>TopNUniqueSiteStatsTest class.</p>
 * This tests the integration between MultiWindowDimensionAggregationOperator and TopNUnique Operator
 *
 */
public class TopNUniqueSiteStatsTest {
  private static Logger log = LoggerFactory.getLogger(TopNUniqueTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
    public void testNodeProcessing() throws Exception {
      testNodeProcessingSchema(new TopNUnique<String, DimensionObject<String>>());

    }

  @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testNodeProcessingSchema(TopNUnique oper) {
      CollectorTestSink sortSink = new CollectorTestSink();
      oper.top.setSink(sortSink);
      oper.setN(3);

      oper.beginWindow(0);
      HashMap<String, DimensionObject<String>> input = new HashMap<String, DimensionObject<String>>();

      input.put("url", new DimensionObject<String>(10, "abc"));
      oper.data.process(input);

      input.clear();
      input.put("url", new DimensionObject<String>(1, "def"));
      oper.data.process(input);

      input.clear();
      input.put("url", new DimensionObject<String>(101, "ghi"));
      oper.data.process(input);

      input.clear();
      input.put("url", new DimensionObject<String>(50, "jkl"));
      oper.data.process(input);

      input.clear();
      input.put("url", new DimensionObject<String>(50, "jkl"));
      oper.data.process(input);
      oper.endWindow();

      Assert.assertEquals("number emitted tuples", 1,	sortSink.collectedTuples.size());
      for (Object o : sortSink.collectedTuples) {
        log.debug(o.toString());
        for (Map.Entry<String, ArrayList<HashMap<DimensionObject<String>, Integer>>> e : ((HashMap<String, ArrayList<HashMap<DimensionObject<String>, Integer>>>) o).entrySet()) {
          if (e.getKey().equals("url")) {
            Assert.assertEquals("emitted value for 'a' was ", 3, e.getValue()
                                .size());
          }
          log.debug(String.format("Sorted list for %s:", e.getKey()));
          for (HashMap<DimensionObject<String>,Integer> i: e.getValue()) {
            log.debug(String.format("%s", i.keySet().toString()));
          }
        }
      }
      log.debug("Done testing round\n");
    }

}
