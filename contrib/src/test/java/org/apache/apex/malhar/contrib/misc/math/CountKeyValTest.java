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
package org.apache.apex.malhar.contrib.misc.math;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 *
 * Functional tests for {@link CountKeyVal}. <p>
 * @deprecated
 */
@Deprecated
public class CountKeyValTest
{
  /**
   * Test operator logic emits correct results.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing()
  {
    CountKeyVal<String, Double> oper = new CountKeyVal<String, Double>();
    CollectorTestSink countSink = new CollectorTestSink();
    oper.count.setSink(countSink);

    oper.beginWindow(0); //

    oper.data.process(new KeyValPair("a", 2.0));
    oper.data.process(new KeyValPair("b", 20.0));
    oper.data.process(new KeyValPair("c", 1000.0));
    oper.data.process(new KeyValPair("a", 1.0));
    oper.data.process(new KeyValPair("a", 10.0));
    oper.data.process(new KeyValPair("b", 5.0));
    oper.data.process(new KeyValPair("d", 55.0));
    oper.data.process(new KeyValPair("b", 12.0));
    oper.data.process(new KeyValPair("d", 22.0));
    oper.data.process(new KeyValPair("d", 14.2));
    oper.data.process(new KeyValPair("d", 46.0));
    oper.data.process(new KeyValPair("e", 2.0));
    oper.data.process(new KeyValPair("a", 23.0));
    oper.data.process(new KeyValPair("d", 4.0));

    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 5, countSink.collectedTuples.size());
    for (Object o : countSink.collectedTuples) {
      KeyValPair<String, Integer> e = (KeyValPair<String, Integer>)o;
      Integer val = (Integer)e.getValue();
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", 4, val.intValue());
      } else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", 3, val.intValue());
      } else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", 1, val.intValue());
      } else if (e.getKey().equals("d")) {
        Assert.assertEquals("emitted tuple for 'd' was ", 5, val.intValue());
      } else if (e.getKey().equals("e")) {
        Assert.assertEquals("emitted tuple for 'e' was ", 1, val.intValue());
      }
    }
  }
}
