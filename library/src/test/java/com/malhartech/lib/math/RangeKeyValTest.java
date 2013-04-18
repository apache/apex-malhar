/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.tuple.Tuple;
import com.malhartech.lib.util.HighLow;
import com.malhartech.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.RangeKeyVal}<p>
 *
 */
public class RangeKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(RangeKeyValTest.class);

  class TestSink implements Sink
  {
    double low = -1;
    double high = -1;

    @Override
    public void process(Object payload)
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
