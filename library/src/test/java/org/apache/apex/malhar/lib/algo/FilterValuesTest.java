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
package org.apache.apex.malhar.lib.algo;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.algo.FilterValues}<p>
 *
 */
public class FilterValuesTest
{
  @SuppressWarnings({ "rawtypes", "unchecked" })
  int getTotal(List list)
  {
    ArrayList<Integer> ilist = (ArrayList<Integer>)list;
    int ret = 0;
    for (Integer i : ilist) {
      ret += i;
    }
    return ret;
  }

  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    FilterValues<Integer> oper = new FilterValues<Integer>();

    CollectorTestSink sortSink = new CollectorTestSink();
    oper.filter.setSink(sortSink);
    Integer[] values = new Integer[2];
    oper.setValue(5);
    oper.clearValues();
    values[0] = 200;
    values[1] = 2;
    oper.setValue(4);
    oper.setValues(values);

    oper.beginWindow(0);
    oper.data.process( 2);
    oper.data.process(5);
    oper.data.process(7);
    oper.data.process(42);
    oper.data.process(200);
    oper.data.process(2);
    Assert.assertEquals("number emitted tuples", 3, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 204, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    oper.data.process(5);
    Assert.assertEquals("number emitted tuples", 0, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 0, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    oper.data.process(2);
    oper.data.process(33);
    oper.data.process(2);
    Assert.assertEquals("number emitted tuples", 2, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 4, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    oper.data.process(6);
    oper.data.process(2);
    oper.data.process(6);
    oper.data.process(2);
    oper.data.process(6);
    oper.data.process(2);
    oper.data.process(6);
    oper.data.process(2);
    Assert.assertEquals("number emitted tuples", 4, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 8, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    oper.setInverse(true);
    oper.data.process(9);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 9, getTotal(sortSink.collectedTuples));

    oper.endWindow();
  }
}
