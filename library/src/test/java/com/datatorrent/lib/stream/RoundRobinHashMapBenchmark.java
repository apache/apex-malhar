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
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.RoundRobinHashMap;
import com.datatorrent.lib.testbench.CountTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.datatorrent.lib.testbench.RoundRobinHashMap}<p>
 */
public class RoundRobinHashMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(RoundRobinHashMapBenchmark.class);

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    RoundRobinHashMap oper = new RoundRobinHashMap();
    CountTestSink mapSink = new CountTestSink();

    String[] keys = new String[3];
    keys[0] = "a";
    keys[1] = "b";
    keys[2] = "c";

    oper.setKeys(keys);
    oper.map.setSink(mapSink);
    oper.beginWindow(0);

    int numtuples = 100000000;
    for (int i = 0; i < numtuples; i++) {
      oper.data.process(i);
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", numtuples / 3, mapSink.getCount());
    log.debug(String.format("\nprocessed %d tuples", numtuples));
  }
}
