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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.testbench;

import com.datatorrent.api.Sink;
import com.datatorrent.lib.testbench.SeedEventClassifier;
import com.datatorrent.tuple.Tuple;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.datatorrent.lib.testbench.SeedEventClassifier}<br>
 */
public class SeedEventClassifierBenchmark
{
  private static Logger LOG = LoggerFactory.getLogger(SeedEventClassifierBenchmark.class);

  class StringSink implements Sink
  {
    int count = 0;

    @Override
    public void put(Object tuple)
    {
      if (tuple instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
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
    int count = 0;

    @Override
    public void put(Object tuple)
    {
      if (tuple instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
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
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
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
      LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", classifySink.count));
    }
    else {
      LOG.debug(String.format("\n********************\nProcessed %d tuples\n********************\n",  hashSink.count));
    }
  }
}
