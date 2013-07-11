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
 * limitations under the License.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.KeyValPairToHashMap;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.lib.util.KeyValPair;

import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.testbench.KeyValPairToHashMap}<p>
 * <br>
 */
public class KeyValPairToHashMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(KeyValPairToHashMapBenchmark.class);

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    KeyValPairToHashMap oper = new KeyValPairToHashMap();
    CountTestSink mapSink = new CountTestSink();

    oper.map.setSink(mapSink);

    oper.beginWindow(0);
    KeyValPair<String, String> input = new KeyValPair<String, String>("a", "1");

    // Same input object can be used as the oper is just pass through
    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.keyval.process(input);
    }

    oper.endWindow();
    log.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", numTuples));
  }
}
