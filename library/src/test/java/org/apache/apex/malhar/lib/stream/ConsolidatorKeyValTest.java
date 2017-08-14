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

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * Functional test for {@link org.apache.apex.malhar.lib.stream.ArrayListAggregator}
 */
public class ConsolidatorKeyValTest
{
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    ConsolidatorKeyVal<String, Integer, Double, Integer, Integer, Integer> oper =
        new ConsolidatorKeyVal<String, Integer, Double, Integer, Integer, Integer>();
    CollectorTestSink cSink = new CollectorTestSink();
    oper.out.setSink(cSink);

    oper.beginWindow(0);
    KeyValPair<String, Integer> m1 = new KeyValPair<String, Integer>("a",1);
    oper.in1.process(m1);
    KeyValPair<String, Double> m2 = new KeyValPair<String, Double>("a",1.0);
    oper.in2.process(m2);
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 1, cSink.collectedTuples.size());


    HashMap<String, ArrayList<Object>> map = (HashMap<String, ArrayList<Object>>)cSink.collectedTuples.get(0);
    Assert.assertEquals("size of sink map", 1, map.size());
  }
}
