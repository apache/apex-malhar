/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestCountAndLastTupleSink;
import com.malhartech.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.MaxKeyVal}<p>
 *
 */
public class MaxKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(MaxKeyValTest.class);

  /**
   * Test functional logic
   */
  @Test
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new MaxKeyVal<String, Integer>(), "integer");
    testSchemaNodeProcessing(new MaxKeyVal<String, Double>(), "double");
    testSchemaNodeProcessing(new MaxKeyVal<String, Long>(), "long");
    testSchemaNodeProcessing(new MaxKeyVal<String, Short>(), "short");
    testSchemaNodeProcessing(new MaxKeyVal<String, Float>(), "float");
  }

  /**
   * Test operator logic emits correct results for each schema.
   *
   */
  public void testSchemaNodeProcessing(MaxKeyVal oper, String type)
  {
    TestCountAndLastTupleSink maxSink = new TestCountAndLastTupleSink();
    oper.max.setSink(maxSink);

    oper.beginWindow(0);

    int numtuples = 10000;
    // For benchmark do -> numtuples = numtuples * 100;
    if (type.equals("integer")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Integer(i)));
      }
    }
    else if (type.equals("double")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Double(i)));
      }
    }
    else if (type.equals("long")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Long(i)));
      }
    }
    else if (type.equals("short")) {
      int count = numtuples / 1000; // cannot cross 64K
      for (short j = 0; j < count; j++) {
        oper.data.process(new KeyValPair("a", new Short(j)));
      }
    }
    else if (type.equals("float")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", new Float(i)));
      }
    }

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, maxSink.count);
    Number val = ((KeyValPair<String, Number>)maxSink.tuple).getValue();
    if (type.equals("short")) {
      Assert.assertEquals("emitted min value was ", new Double(numtuples / 1000 - 1), val);
    }
    else {
      Assert.assertEquals("emitted min value was ", new Double(numtuples - 1), val);
    }
  }
}
