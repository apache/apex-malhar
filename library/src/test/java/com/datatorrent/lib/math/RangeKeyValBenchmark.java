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

import com.datatorrent.api.Sink;
import com.datatorrent.lib.math.RangeKeyVal;
import com.datatorrent.lib.util.HighLow;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.tuple.Tuple;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.RangeKeyVal}<p>
 *
 */
public class RangeKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(RangeKeyValBenchmark.class);

  class TestSink implements Sink
  {
    double low = -1;
    double high = -1;

    @Override
    public void put(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        KeyValPair<String, Object> tuple = (KeyValPair<String, Object>)payload;
        HighLow hl = (HighLow)tuple.getValue();
        high = hl.getHigh().doubleValue();
        low = hl.getLow().doubleValue();
      }
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
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new RangeKeyVal<String, Integer>(), "integer"); // 8million/s
    testSchemaNodeProcessing(new RangeKeyVal<String, Double>(), "double"); // 8 million/s
    testSchemaNodeProcessing(new RangeKeyVal<String, Long>(), "long"); // 8 million/s
    testSchemaNodeProcessing(new RangeKeyVal<String, Short>(), "short"); // 8 million/s
    testSchemaNodeProcessing(new RangeKeyVal<String, Float>(), "float"); // 8 million/s
  }

  /**
   * Test node logic emits correct results for each schema
   */
  public void testSchemaNodeProcessing(RangeKeyVal node, String type)
  {
    TestSink rangeSink = new TestSink();
    node.range.setSink(rangeSink);

    int numtuples = 100000000; // 100 millions
    if (type.equals("integer")) {
      KeyValPair<String, Integer> kv = new KeyValPair<String, Integer>("a", new Integer(0));
      for (int i = -10; i < numtuples; i++) {
        kv.setValue(new Integer(i));
        node.data.process(kv);
      }
    }
    else if (type.equals("double")) {
      KeyValPair<String, Double> kv = new KeyValPair<String, Double>("a", new Double(0.0));
      for (int i = -10; i < numtuples; i++) {
        kv.setValue(new Double(i));
        node.data.process(kv);
      }
    }
    else if (type.equals("long")) {
      KeyValPair<String, Long> kv = new KeyValPair<String, Long>("a", new Long(0));
      for (int i = -10; i < numtuples; i++) {
        kv.setValue(new Long(i));
        node.data.process(kv);
      }
    }
    else if (type.equals("short")) {
      short s = 0;
      KeyValPair<String, Short> kv = new KeyValPair<String, Short>("a", new Short(s));
      int count = numtuples / 1000; // cannot cross 64K
      for (int j = 0; j < count; j++) {
        for (short i = -10; i < 1000; i++) {
          kv.setValue(new Short(i));
          node.data.process(kv);
        }
      }
    }
    else if (type.equals("float")) {
      KeyValPair<String, Float> kv = new KeyValPair<String, Float>("a", new Float(0));
      for (int i = -10; i < numtuples; i++) {
        kv.setValue(new Float(i));
        node.data.process(kv);
      }
    }

    node.endWindow();
    log.debug(String.format("\n****************************\nThe high is %f, and low is %f from %d tuples of type %s\n*************************\n",
                            rangeSink.high, rangeSink.low, numtuples, type));
  }
}
