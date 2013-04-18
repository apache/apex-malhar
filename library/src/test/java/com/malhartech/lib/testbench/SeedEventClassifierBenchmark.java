/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.Sink;
import com.malhartech.tuple.Tuple;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.SeedEventClassifier}<br>
 */
public class SeedEventClassifierBenchmark
{
  private static Logger LOG = LoggerFactory.getLogger(SeedEventClassifierBenchmark.class);

  class StringSink implements Sink
  {
    int count = 0;

    @Override
    public void process(Object tuple)
    {
      if (tuple instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
        count++;
      }
    }
  }

  class HashSink implements Sink
  {
    int count = 0;

    @Override
    public void process(Object tuple)
    {
      if (tuple instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
         count++;
      }
    }
  }

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testSchemaNodeProcessing(true); // 13 million/sec
    testSchemaNodeProcessing(false); // 7 million/sec
  }

  /**
   * Test oper logic emits correct results
   */
  public void testSchemaNodeProcessing(boolean isstring) throws Exception
  {
    SeedEventClassifier oper = new SeedEventClassifier();
    StringSink classifySink = new StringSink();
    HashSink hashSink = new HashSink();

    Sink inSink1 = oper.data1.getSink();
    Sink inSink2 = oper.data2.getSink();
    if (isstring) {
      oper.string_data.setSink(classifySink);
    }
    else {
      oper.hash_data.setSink(hashSink);
    }

    oper.setKey1("x");
    oper.setKey2("y");
    oper.setSeedstart(0);
    oper.setSeedend(1000);
    oper.setup(null);

    oper.beginWindow(0);

    int numTuples = 10000000;
    if (isstring) {
      String input;
      for (int j = 0; j < 5; j++) {
        for (int i = 0; i < numTuples; i++) {
          input = Integer.toString(i);
          inSink1.process(input);
          inSink2.process(input);
        }
      }
    }
    else {
      Integer input;
      for (int j = 0; j < 5; j++) {
        for (int i = 0; i < numTuples; i++) {
          input = new Integer(i);
          inSink1.process(input);
          inSink2.process(input);
        }
      }
    }
    oper.endWindow();
    if (isstring) {
      LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", classifySink.count));
    }
    else {
      LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n",  hashSink.count));
    }
  }
}
