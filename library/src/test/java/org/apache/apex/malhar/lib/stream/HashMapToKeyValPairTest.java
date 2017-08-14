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

import org.apache.apex.malhar.lib.testbench.CountTestSink;

/**
 * Functional test for {@link org.apache.apex.malhar.lib.stream.HashMapToKeyValPair}
 */
public class HashMapToKeyValPairTest
{

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    HashMapToKeyValPair oper = new HashMapToKeyValPair();
    CountTestSink keySink = new CountTestSink();
    CountTestSink valSink = new CountTestSink();
    CountTestSink keyvalSink = new CountTestSink();

    oper.key.setSink(keySink);
    oper.val.setSink(valSink);
    oper.keyval.setSink(keyvalSink);

    oper.beginWindow(0);
    HashMap<String, String> input = new HashMap<String, String>();
    input.put("a", "1");
    // Same input object can be used as the oper is just pass through
    int numtuples = 1000;
    for (int i = 0; i < numtuples; i++) {
      oper.data.process(input);
    }

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", numtuples, keySink.count);
    Assert.assertEquals("number emitted tuples", numtuples, valSink.count);
    Assert.assertEquals("number emitted tuples", numtuples, keyvalSink.count);
  }
}
