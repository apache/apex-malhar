/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.engine.TestCountSink;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.StreamDuplicater}<p>
 * Benchmarks: Currently does about ?? Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class ArrayListToItemTest {

    private static Logger log = LoggerFactory.getLogger(ArrayListToItemTest.class);

    /**
     * Test oper pass through. The Object passed is not relevant
     */
    @Test
    public void testNodeProcessing() throws Exception
    {
      ArrayListToItem oper = new ArrayListToItem();
      TestCountSink itemSink = new TestCountSink();
      oper.item.setSink(itemSink);
      oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));

      oper.beginWindow(0);
      ArrayList<String> input = new ArrayList<String>();
      input.add("a");
      // Same input object can be used as the oper is just pass through
      int numtuples = 1000;
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(input);
      }

      oper.endWindow();
      Assert.assertEquals("number emitted tuples", numtuples, itemSink.count);
    }
}
