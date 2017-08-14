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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.testbench.ThroughputCounter}. <p>
 * <br>
 * Load is generated and the tuples are outputted to ensure that the numbers are roughly in line with the weights<br>
 * <br>
 *  Benchmarks:<br>
 * String schema generates over 11 Million tuples/sec<br>
 * HashMap schema generates over 1.7 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 *
 */
public class ThroughputCounterTest
{

  private static Logger log = LoggerFactory.getLogger(ThroughputCounterTest.class);

  @SuppressWarnings("rawtypes")
  class TestCountSink implements Sink
  {
    long count = 0;
    long average = 0;

    /**
     * @param payload
     */
    @SuppressWarnings("unchecked")
    @Override
    public void put(Object payload)
    {
      HashMap<String, Number> tuples = (HashMap<String, Number>)payload;
      average = (Long)tuples.get(ThroughputCounter.OPORT_COUNT_TUPLE_AVERAGE);
      count += (Long)tuples.get(ThroughputCounter.OPORT_COUNT_TUPLE_COUNT);
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  /**
   * Tests both string and non string schema
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testSingleSchemaNodeProcessing() throws Exception
  {
    ThroughputCounter<String, Integer> node = new ThroughputCounter<String, Integer>();

    TestCountSink countSink = new TestCountSink();
    node.count.setSink(countSink);
    node.setRollingWindowCount(5);
    node.setup(null);

    node.beginWindow(0);
    HashMap<String, Integer> input;
    int aint = 1000;
    int bint = 100;
    Integer aval = aint;
    Integer bval = bint;
    long ntot = aint + bint;
    long numtuples = 1000;
    long sentval = 0;
    for (long i = 0; i < numtuples; i++) {
      input = new HashMap<String, Integer>();
      input.put("a", aval);
      input.put("b", bval);
      sentval += 2;
      node.data.process(input);
    }
    node.endWindow();

    log.info(String.format(
        "\n*******************************************************\nGot average per sec(%d), count(got %d, expected " + "%d), numtuples(%d)",
        countSink.average,
        countSink.count,
        ntot * numtuples,
        sentval));
  }
}
