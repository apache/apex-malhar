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
package org.apache.apex.malhar.lib.math;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import com.datatorrent.common.util.Pair;

/**
 * Functional tests for {@link org.apache.apex.malhar.lib.math.LogicalCompare}
 */
public class LogicalCompareTest
{
  /**
   * Test operator logic emits correct results.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing()
  {
    LogicalCompare<Integer> oper = new LogicalCompare<Integer>()
    {
    };
    CollectorTestSink eSink = new CollectorTestSink();
    CollectorTestSink neSink = new CollectorTestSink();
    CollectorTestSink gtSink = new CollectorTestSink();
    CollectorTestSink gteSink = new CollectorTestSink();
    CollectorTestSink ltSink = new CollectorTestSink();
    CollectorTestSink lteSink = new CollectorTestSink();

    oper.equalTo.setSink(eSink);
    oper.notEqualTo.setSink(neSink);
    oper.greaterThan.setSink(gtSink);
    oper.greaterThanOrEqualTo.setSink(gteSink);
    oper.lessThan.setSink(ltSink);
    oper.lessThanOrEqualTo.setSink(lteSink);

    Pair<Integer, Integer> gtuple = new Pair<Integer, Integer>(2, 1);
    Pair<Integer, Integer> etuple = new Pair<Integer, Integer>(2, 2);
    Pair<Integer, Integer> ltuple = new Pair<Integer, Integer>(2, 3);

    oper.beginWindow(0); //

    oper.input.process(gtuple);
    oper.input.process(etuple);
    oper.input.process(ltuple);

    oper.endWindow(); //

    Assert.assertEquals("number emitted tuples", 1,
        eSink.collectedTuples.size());
    Assert.assertEquals("tuples were",
        eSink.collectedTuples.get(0).equals(etuple), true);

    Assert.assertEquals("number emitted tuples", 2,
        neSink.collectedTuples.size());
    Assert.assertEquals("tuples were",
        neSink.collectedTuples.get(0).equals(gtuple), true);
    Assert.assertEquals("tuples were",
        neSink.collectedTuples.get(1).equals(ltuple), true);

    Assert.assertEquals("number emitted tuples", 1,
        gtSink.collectedTuples.size());
    Assert.assertEquals("tuples were",
        gtSink.collectedTuples.get(0).equals(gtuple), true);

    Assert.assertEquals("number emitted tuples", 2,
        gteSink.collectedTuples.size());
    Assert.assertEquals("tuples were",
        gteSink.collectedTuples.get(0).equals(gtuple), true);
    Assert.assertEquals("tuples were",
        gteSink.collectedTuples.get(1).equals(etuple), true);

    Assert.assertEquals("number emitted tuples", 1,
        ltSink.collectedTuples.size());
    Assert.assertEquals("tuples were",
        ltSink.collectedTuples.get(0).equals(ltuple), true);

    Assert.assertEquals("number emitted tuples", 2,
        lteSink.collectedTuples.size());
    Assert.assertEquals("tuples were",
        lteSink.collectedTuples.get(0).equals(etuple), true);
    Assert.assertEquals("tuples were",
        lteSink.collectedTuples.get(1).equals(ltuple), true);
  }
}
