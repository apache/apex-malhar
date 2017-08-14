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
package org.apache.apex.malhar.contrib.misc.algo;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * @deprecated
 * Functional tests for {@link InsertSortDesc}<p>
 */
@Deprecated
public class InsertSortDescTest
{
  private static Logger log = LoggerFactory.getLogger(InsertSortDescTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new InsertSortDesc<Integer>(), "Integer");
    testNodeProcessingSchema(new InsertSortDesc<Double>(), "Double");
    testNodeProcessingSchema(new InsertSortDesc<Float>(), "Float");
    testNodeProcessingSchema(new InsertSortDesc<String>(), "String");
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testNodeProcessingSchema(InsertSortDesc oper, String debug)
  {
    //FirstN<String,Float> aoper = new FirstN<String,Float>();
    CollectorTestSink sortSink = new CollectorTestSink();
    CollectorTestSink hashSink = new CollectorTestSink();
    oper.sort.setSink(sortSink);
    oper.sorthash.setSink(hashSink);

    ArrayList input = new ArrayList();

    oper.beginWindow(0);

    input.add(2);
    oper.datalist.process(input);
    oper.data.process(20);

    input.clear();
    input.add(1000);
    input.add(5);
    input.add(20);
    input.add(33);
    input.add(33);
    input.add(34);
    oper.datalist.process(input);

    input.clear();
    input.add(34);
    input.add(1001);
    input.add(6);
    input.add(1);
    input.add(33);
    input.add(9);
    oper.datalist.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1, hashSink.collectedTuples.size());
    HashMap map = (HashMap)hashSink.collectedTuples.get(0);
    input = (ArrayList)sortSink.collectedTuples.get(0);
    for (Object o : input) {
      log.debug(String.format("%s : %s", o.toString(), map.get(o).toString()));
    }
    log.debug(String.format("Tested %s type with %d tuples and %d uniques\n", debug, input.size(), map.size()));
  }
}
