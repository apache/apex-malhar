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

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CountTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * Functional test for {@link org.apache.apex.malhar.lib.stream.KeyValPairToHashMap}
 * <p>
 * <br>
 */
public class KeyPairToHashMapTest
{

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    KeyValPairToHashMap oper = new KeyValPairToHashMap();
    CountTestSink mapSink = new CountTestSink();

    oper.map.setSink(mapSink);

    oper.beginWindow(0);
    KeyValPair<String, String> input = new KeyValPair<String, String>("a", "1");

    // Same input object can be used as the oper is just pass through
    int numtuples = 1000;
    for (int i = 0; i < numtuples; i++) {
      oper.keyval.process(input);
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", numtuples, mapSink.count);
  }
}
