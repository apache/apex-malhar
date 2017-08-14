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

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.algo.TopNUnique;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 * <p>TopNUniqueSiteStatsTest class.</p>
 * This tests the integration between MultiWindowDimensionAggregationOperator and TopNUnique Operator
 *
 */
public class TopNUniqueSiteStatsTest
{
  private static Logger log = LoggerFactory.getLogger(TopNUniqueSiteStatsTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new TopNUnique<String, DimensionObject<String>>());

  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testNodeProcessingSchema(TopNUnique oper)
  {
    CollectorTestSink sortSink = new CollectorTestSink();
    oper.top.setSink(sortSink);
    oper.setN(3);

    oper.beginWindow(0);
    HashMap<String, DimensionObject<String>> input = new HashMap<String, DimensionObject<String>>();

    input.put("url", new DimensionObject<String>(new MutableDouble(10), "abc"));
    oper.data.process(input);

    input.clear();
    input.put("url", new DimensionObject<String>(new MutableDouble(1), "def"));
    input.put("url1", new DimensionObject<String>(new MutableDouble(1), "def"));
    oper.data.process(input);

    input.clear();
    input.put("url", new DimensionObject<String>(new MutableDouble(101), "ghi"));
    input.put("url1", new DimensionObject<String>(new MutableDouble(101), "ghi"));
    oper.data.process(input);

    input.clear();
    input.put("url", new DimensionObject<String>(new MutableDouble(50), "jkl"));
    oper.data.process(input);

    input.clear();
    input.put("url", new DimensionObject<String>(new MutableDouble(50), "jkl"));
    input.put("url3", new DimensionObject<String>(new MutableDouble(50), "jkl"));
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      log.debug(o.toString());
    }
    log.debug("Done testing round\n");
  }

}
