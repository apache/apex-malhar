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
package org.apache.apex.malhar.lib.math;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.HighLow;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.Sink;

/**
 * Functional tests for {@link org.apache.apex.malhar.lib.math.RangeKeyVal}<p>
 */
public class RangeKeyValTest<V extends Number>
{
  private static Logger log = LoggerFactory.getLogger(RangeKeyValTest.class);

  /**
   * Test functional logic
   */
  @Test
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new RangeKeyVal<String, Integer>(), "integer");
    testSchemaNodeProcessing(new RangeKeyVal<String, Double>(), "double");
    testSchemaNodeProcessing(new RangeKeyVal<String, Long>(), "long");
    testSchemaNodeProcessing(new RangeKeyVal<String, Short>(), "short");
    testSchemaNodeProcessing(new RangeKeyVal<String, Float>(), "float");
  }

  /**
   * Test node logic emits correct results for each schema
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testSchemaNodeProcessing(RangeKeyVal node, String type)
  {
    TestSink rangeSink = new TestSink();
    node.range.setSink(rangeSink);

    int numtuples = 1000;
    if (type.equals("integer")) {
      for (int i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Integer>("a", i));
      }
    } else if (type.equals("double")) {
      for (int i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Double>("a", (double)i));
      }
    } else if (type.equals("long")) {
      for (int i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Long>("a", (long)i));
      }
    } else if (type.equals("short")) {
      for (short i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Short>("a", i));
      }
    } else if (type.equals("float")) {
      for (int i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Float>("a", (float)i));
      }
    }

    node.endWindow();
    Assert.assertEquals("high was ", 999.0, rangeSink.high, 0);
    Assert.assertEquals("low was ", -10.0, rangeSink.low, 0);
    log.debug(String.format("\nTested %d tuples", numtuples));
  }

  @SuppressWarnings("rawtypes")
  class TestSink implements Sink
  {
    double low = -1;
    double high = -1;

    @SuppressWarnings("unchecked")
    @Override
    public void put(Object payload)
    {
      KeyValPair<String, Object> tuple = (KeyValPair<String, Object>)payload;
      HighLow<V> hl = (HighLow<V>)tuple.getValue();
      high = hl.getHigh().doubleValue();
      low = hl.getLow().doubleValue();
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
