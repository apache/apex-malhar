/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.testbench;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;

/**
 * Functional test for {@link org.apache.apex.malhar.lib.testbench.SeedEventClassifier}<br>
 */
public class SeedEventClassifierTest
{
  private static Logger LOG = LoggerFactory.getLogger(SeedEventClassifierTest.class);

  @SuppressWarnings("rawtypes")
  class StringSink implements Sink
  {
    HashMap<String, String> collectedTuples = new HashMap<String, String>();
    int count = 0;

    @Override
    public void put(Object tuple)
    {
      collectedTuples.put((String)tuple, null);
      count++;
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  @SuppressWarnings("rawtypes")
  class HashSink implements Sink
  {
    HashMap<HashMap<String, HashMap<String, Integer>>, String> collectedTuples = new HashMap<HashMap<String, HashMap<String, Integer>>, String>();
    int count = 0;

    @SuppressWarnings("unchecked")
    @Override
    public void put(Object tuple)
    {
      collectedTuples.put((HashMap<String, HashMap<String, Integer>>)tuple, null);
      count++;
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
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testSchemaNodeProcessing(boolean isstring) throws Exception
  {
    final SeedEventClassifier oper = new SeedEventClassifier();
    StringSink classifySink = new StringSink();
    HashSink hashSink = new HashSink();

    Sink inSink1 = oper.data1.getSink();
    Sink inSink2 = oper.data2.getSink();
    if (isstring) {
      oper.string_data.setSink(classifySink);
    } else {
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
    } else {
      Integer input;
      for (int j = 0; j < 5; j++) {
        for (int i = 0; i < numTuples; i++) {
          input = i;
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
    } else {
      Assert.assertEquals("number emitted tuples", numTuples * 2 * 5, hashSink.count);
      LOG.debug(String.format("\n********************\nProcessed %d tuples with %d uniques\n********************\n",
          hashSink.count,
          hashSink.collectedTuples.size()));
    }
  }
}
