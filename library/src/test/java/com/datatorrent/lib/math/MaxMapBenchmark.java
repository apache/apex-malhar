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

import com.datatorrent.lib.math.MaxMap;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.MaxMap}. <p>
 *
 */
public class MaxMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MaxMapBenchmark.class);

  /**
   * Test functional logic
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new MaxMap<String, Integer>(), "integer"); // 8million/s
    testSchemaNodeProcessing(new MaxMap<String, Double>(), "double"); // 8 million/s
    testSchemaNodeProcessing(new MaxMap<String, Long>(), "long"); // 8 million/s
    testSchemaNodeProcessing(new MaxMap<String, Short>(), "short"); // 8 million/s
    testSchemaNodeProcessing(new MaxMap<String, Float>(), "float"); // 8 million/s
  }

  /**
   * Test oper logic emits correct results for each schema
   */
  public void testSchemaNodeProcessing(MaxMap oper, String type)
  {
    CountAndLastTupleTestSink maxSink = new CountAndLastTupleTestSink();
    oper.max.setSink(maxSink);

    oper.beginWindow(0);

    HashMap<String, Number> input = new HashMap<String, Number>();
    int numtuples = 100000000;
    // For benchmark do -> numtuples = numtuples * 100;
    if (type.equals("integer")) {
      HashMap<String, Integer> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Integer>();
        tuple.put("a", new Integer(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("double")) {
      HashMap<String, Double> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Double>();
        tuple.put("a", new Double(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("long")) {
      HashMap<String, Long> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Long>();
        tuple.put("a", new Long(i));
        oper.data.process(tuple);
      }
    }
    else if (type.equals("short")) {
      HashMap<String, Short> tuple;
      int count = numtuples / 1000; // cannot cross 64K
      for (int j = 0; j < count; j++) {
        for (short i = 0; i < 1000; i++) {
          tuple = new HashMap<String, Short>();
          tuple.put("a", new Short(i));
          oper.data.process(tuple);
        }
      }
    }
    else if (type.equals("float")) {
      HashMap<String, Float> tuple;
      for (int i = 0; i < numtuples; i++) {
        tuple = new HashMap<String, Float>();
        tuple.put("a", new Float(i));
        oper.data.process(tuple);
      }
    }
    oper.endWindow();

    HashMap<String, Number> shash = (HashMap<String, Number>)maxSink.tuple;
    Number val = shash.get("a");
    log.debug(String.format("\nBenchmark total for %d tuples; expected 0.0, got %f", numtuples, val));
  }
}
