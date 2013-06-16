/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.ArrayListToItem;
import com.datatorrent.lib.testbench.CountTestSink;

import java.util.ArrayList;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.testbench.StreamDuplicater}<p>
 * Benchmarks: Currently does about ?? Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class ArrayListToItemBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ArrayListToItemBenchmark.class);

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    ArrayListToItem oper = new ArrayListToItem();
    CountTestSink itemSink = new CountTestSink();
    oper.item.setSink(itemSink);

    oper.beginWindow(0);
    ArrayList<String> input = new ArrayList<String>();
    input.add("a");
    // Same input object can be used as the oper is just pass through
    int numTuples = 200000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input);
    }

    oper.endWindow();
    log.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", numTuples));
  }
}
