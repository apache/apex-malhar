/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.lib.math.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.Sum}<p>
 *
 */
public class LineSplitterTest
{
  private static Logger log = LoggerFactory.getLogger(LineSplitterTest.class);

  class TestSink implements Sink
  {
    List<Object> collectedTuples = new ArrayList<Object>();

    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        collectedTuples.add(payload);
      }
    }
  }

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeProcessing()
  {

    LineSplitter oper = new LineSplitter();
    TestSink sumSink = new TestSink();
    TestSink countSink = new TestSink();
    TestSink averageSink = new TestSink();

    oper.setSplitby(",");
    oper.tokens.setSink(sumSink);


    // Not needed, but still setup is being called as a matter of discipline
    oper.setup(new com.malhartech.dag.OperatorContext("irrelevant", null));
    oper.beginWindow(0); //

    String input = "a,b,c";

    oper.data.process(input);


    oper.endWindow(); //
      Assert.assertEquals("number emitted tuples", 1, averageSink.collectedTuples.size());

  }
}
