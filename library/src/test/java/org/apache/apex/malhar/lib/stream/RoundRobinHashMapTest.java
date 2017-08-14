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
package org.apache.apex.malhar.lib.stream;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link org.apache.apex.malhar.lib.stream.RoundRobinHashMap}.
 */
public class RoundRobinHashMapTest
{

  private static Logger log = LoggerFactory
      .getLogger(RoundRobinHashMapTest.class);

  /**
   * Test operator pass through. The Object passed is not relevant
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    RoundRobinHashMap oper = new RoundRobinHashMap();
    CollectorTestSink mapSink = new CollectorTestSink();

    String[] keys = new String[3];
    keys[0] = "a";
    keys[1] = "b";
    keys[2] = "c";

    oper.setKeys(keys);
    oper.map.setSink(mapSink);
    oper.beginWindow(0);

    HashMap<String, Integer> t1 = new HashMap<String, Integer>();
    t1.put("a", 0);
    t1.put("b", 1);
    t1.put("c", 2);
    HashMap<String, Integer> t2 = new HashMap<String, Integer>();
    t2.put("a", 3);
    t2.put("b", 4);
    t2.put("c", 5);
    HashMap<String, Integer> t3 = new HashMap<String, Integer>();
    t3.put("a", 6);
    t3.put("b", 7);
    t3.put("c", 8);

    HashMap<String, Integer> t4 = new HashMap<String, Integer>();
    t4.put("a", 9);
    t4.put("b", 10);
    t4.put("c", 11);

    // Same input object can be used as the oper is just pass through
    int numtuples = 12;
    for (int i = 0; i < numtuples; i++) {
      oper.data.process(i);
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", numtuples / 3,
        mapSink.collectedTuples.size());
    log.debug(mapSink.collectedTuples.toString());
    Assert.assertEquals("tuple 1", t1, mapSink.collectedTuples.get(0));
    Assert.assertEquals("tuple 2", t2, mapSink.collectedTuples.get(1));
    Assert.assertEquals("tuple 3", t3, mapSink.collectedTuples.get(2));
    Assert.assertEquals("tuple 4", t4, mapSink.collectedTuples.get(3));
  }
}
