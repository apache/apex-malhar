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
package com.datatorrent.lib.math;

import com.datatorrent.api.Sink;
import com.datatorrent.lib.math.RangeKeyVal;
import com.datatorrent.lib.util.HighLow;
import com.datatorrent.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.RangeKeyVal}<p>
 *
 */
public class RangeKeyValTest<V extends Number>
{
  private static Logger log = LoggerFactory.getLogger(RangeKeyValTest.class);

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
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testSchemaNodeProcessing(RangeKeyVal node, String type)
  {
    TestSink rangeSink = new TestSink();
    node.range.setSink(rangeSink);

    int numtuples = 1000;
    if (type.equals("integer")) {
      for (int i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Integer>("a", new Integer(i)));
      }
    }
    else if (type.equals("double")) {
      for (int i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Double>("a", new Double(i)));
      }
    }
    else if (type.equals("long")) {
      for (int i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Long>("a", new Long(i)));
      }
    }
    else if (type.equals("short")) {
      for (short i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Short>("a", new Short(i)));
      }
    }
    else if (type.equals("float")) {
      for (int i = -10; i < numtuples; i++) {
        node.data.process(new KeyValPair<String, Float>("a", new Float(i)));
      }
    }

    node.endWindow();
    Assert.assertEquals("high was ", new Double(999.0), rangeSink.high);
    Assert.assertEquals("low was ", new Double(-10.0), rangeSink.low);
    log.debug(String.format("\nTested %d tuples", numtuples));
  }
}
