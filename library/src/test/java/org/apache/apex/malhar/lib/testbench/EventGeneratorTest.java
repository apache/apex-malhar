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
package org.apache.apex.malhar.lib.testbench;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

import static org.junit.Assert.assertTrue;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.testbench.EventGenerator}. <p>
 * <br>
 * Load is generated and the tuples are outputted to ensure that the numbers are roughly in line with the weights<br>
 * <br>
 * Benchmarks:<br>
 * String schema generates over 11 Million tuples/sec<br>
 * HashMap schema generates over 1.7 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 *
 */
public class EventGeneratorTest
{
  private static Logger log = LoggerFactory.getLogger(EventGeneratorTest.class);
  static int scount = 0;
  static int hcount = 0;
  static int ccount = 0;

  /**
   * Test node logic emits correct results
   */
  public static class CollectorOperator extends BaseOperator
  {
    public final transient DefaultInputPort<String> sdata = new DefaultInputPort<String>()
    {
      @Override
      public void process(String tuple)
      {
        scount++;
      }
    };
    public final transient DefaultInputPort<HashMap<String, Double>> hdata = new DefaultInputPort<HashMap<String, Double>>()
    {
      @Override
      public void process(HashMap<String, Double> tuple)
      {
        hcount++;
      }
    };
    public final transient DefaultInputPort<HashMap<String, Number>> count = new DefaultInputPort<HashMap<String, Number>>()
    {
      @Override
      public void process(HashMap<String, Number> tuple)
      {
        ccount++;
        for (Map.Entry<String, Number> e: tuple.entrySet()) {
          log.debug(String.format("Count data \"%s\" = %f", e.getKey(), e.getValue().doubleValue()));
        }
      }
    };
  }

  /**
   * Tests both string and non string schema
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testSingleSchemaNodeProcessing(true); // 15 million/s
    testSingleSchemaNodeProcessing(false); // 7.5 million/s
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testSingleSchemaNodeProcessing(boolean stringschema) throws Exception
  {
    EventGenerator node = new EventGenerator();
    node.setKeysHelper("a,b,c,d");
    node.setValuesHelper("");
    node.setWeightsHelper("10,40,20,30");
    CollectorTestSink count = new CollectorTestSink();
    node.count.setSink(count);
    CollectorTestSink data = new CollectorTestSink();
    node.string_data.setSink(data);
    CollectorTestSink hashData = new CollectorTestSink();
    node.hash_data.setSink(hashData);

    node.setup(null);
    node.beginWindow(1);
    node.emitTuples();
    node.endWindow();
    node.teardown();

    assertTrue("Default number of tuples generated", 10000 == data.collectedTuples.size());

  }
}
