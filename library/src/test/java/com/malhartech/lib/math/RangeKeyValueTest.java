/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import com.malhartech.lib.util.KeyValPair;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.RangeKeyValue}<p>
 *
 */
public class RangeKeyValueTest
{
  private static Logger log = LoggerFactory.getLogger(RangeKeyValueTest.class);

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
        ArrayList<Number> alist = (ArrayList<Number>)tuple.getValue();
        high = alist.get(0).doubleValue();
        low = alist.get(1).doubleValue();
      }
    }
  }

  /**
   * Test functional logic
   */
  @Test
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new RangeKeyValue<String, Integer>(), "integer");
    testSchemaNodeProcessing(new RangeKeyValue<String, Double>(), "double");
    testSchemaNodeProcessing(new RangeKeyValue<String, Long>(), "long");
    testSchemaNodeProcessing(new RangeKeyValue<String, Short>(), "short");
    testSchemaNodeProcessing(new RangeKeyValue<String, Float>(), "float");
  }

  /**
   * Test node logic emits correct results for each schema
   */
  public void testSchemaNodeProcessing(RangeKeyValue node, String type)
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
