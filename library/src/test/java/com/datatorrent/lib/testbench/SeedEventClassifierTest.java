/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.testbench;

import com.datatorrent.lib.testbench.SeedEventClassifier;
import com.malhartech.api.Sink;
import com.malhartech.tuple.Tuple;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.datatorrent.lib.testbench.SeedEventClassifier}<br>
 */
public class SeedEventClassifierTest
{
  private static Logger LOG = LoggerFactory.getLogger(SeedEventClassifierTest.class);

  class StringSink implements Sink
  {
    HashMap<String, String> collectedTuples = new HashMap<String, String>();
    int count = 0;

    @Override
    public void put(Object tuple)
    {
      if (tuple instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
        collectedTuples.put((String) tuple, null);
        count++;
      }
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  class HashSink implements Sink
  {
    HashMap<HashMap<String, HashMap<String, Integer>>, String> collectedTuples = new HashMap<HashMap<String, HashMap<String, Integer>>, String>();
    int count = 0;

    @Override
    public void put(Object tuple)
    {
      if (tuple instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
         collectedTuples.put((HashMap<String, HashMap<String, Integer>>) tuple, null);
         count++;
      }
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testSchemaNodeProcessing(true); // 5.9 million/sec
    testSchemaNodeProcessing(false); // 4.4 million/sec
  }

  /**
   * Test oper logic emits correct results
   */
  public void testSchemaNodeProcessing(boolean isstring) throws Exception
  {
    final SeedEventClassifier oper = new SeedEventClassifier();
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

    int numTuples = 1000;
    if (isstring) {
      String input;
      for (int j = 0; j < 5; j++) {
        for (int i = 0; i < numTuples; i++) {
          input = Integer.toString(i);
          inSink1.put(input);
          inSink2.put(input);
        }
      }
    }
    else {
      Integer input;
      for (int j = 0; j < 5; j++) {
        for (int i = 0; i < numTuples; i++) {
          input = new Integer(i);
          inSink1.put(input);
          inSink2.put(input);
        }
      }
    }
    oper.endWindow();
    if (isstring) {
      Assert.assertEquals("number emitted tuples", numTuples * 2 * 5, classifySink.count);
      LOG.debug(String.format("\n********************\nProcessed %d tuples with %d uniques\n********************\n",
                              classifySink.count,
                              classifySink.collectedTuples.size()));
    }
    else {
      Assert.assertEquals("number emitted tuples", numTuples * 2 * 5, hashSink.count);
      LOG.debug(String.format("\n********************\nProcessed %d tuples with %d uniques\n********************\n",
                              hashSink.count,
                              hashSink.collectedTuples.size()));
    }
  }
}
