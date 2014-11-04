/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.algo;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

import org.junit.Assert;

import com.datatorrent.lib.testbench.CollectorTestSink;

public class BottomNUnifierTest
{
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testUnifier() {
    
    // Instantiate unifier
    BottomNUnifier<String, Integer> oper = new BottomNUnifier<String, Integer> ();
    oper.setN(2);
    CollectorTestSink sink = new CollectorTestSink();
    oper.mergedport.setSink(sink);
    
    oper.beginWindow(1);
    ArrayList<Integer> values = new ArrayList<Integer>();
    values.add(5);
    values.add(2);
    HashMap<String, ArrayList<Integer>> tuple = new HashMap<String, ArrayList<Integer>>();
    tuple.put("a", values);
    oper.process(tuple);
    values = new ArrayList<Integer>();
    values.add(3);
    values.add(5);
    tuple = new HashMap<String, ArrayList<Integer>>();
    tuple.put("a", values);
    oper.process(tuple);
    oper.endWindow();
    
    Assert.assertEquals("Tuples in sink", sink.collectedTuples.size(), 1);
    tuple = (HashMap<String, ArrayList<Integer>>) sink.collectedTuples.get(0);
    values = tuple.get("a");
    Assert.assertEquals(2, values.size());
    Assert.assertEquals(true, values.indexOf(2) >= 0);
    Assert.assertEquals(true, values.indexOf(3) >= 0);
  }
}
