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
package org.apache.apex.malhar.lib.statistics;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

/**
 * Functional Test for {@link org.apache.apex.malhar.lib.statistics.WeightedMeanOperator}. <br>
 */
public class MeridianOperatorTest
{
  @Test
  public void testWeightedMean()
  {
    MedianOperator oper = new MedianOperator();
    CollectorTestSink<Number> sink = new CollectorTestSink<Number>();
    TestUtils.setSink(oper.median, sink);

    oper.setup(null);
    oper.beginWindow(0);
    oper.data.process(1.0);
    oper.data.process(7.0);
    oper.data.process(3.0);
    oper.data.process(9.0);
    oper.endWindow();

    Assert.assertEquals("Must be one tuple in sink", sink.collectedTuples.size(), 1);
    Assert.assertTrue("Median value", sink.collectedTuples.get(0).doubleValue() == 5.0);
  }
}
