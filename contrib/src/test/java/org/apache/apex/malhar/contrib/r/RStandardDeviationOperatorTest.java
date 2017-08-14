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
package org.apache.apex.malhar.contrib.r;


import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

public class RStandardDeviationOperatorTest
{
  @Test
  public void testWeightedMean()
  {
    RStandardDeviation oper = new RStandardDeviation();
    CollectorTestSink<Object> variance = new CollectorTestSink<Object>();
    oper.variance.setSink(variance);
    CollectorTestSink<Object> deviation = new CollectorTestSink<Object>();
    oper.standardDeviation.setSink(deviation);

    oper.setup(null);
    oper.beginWindow(0);
    oper.data.process(1.0);
    oper.data.process(7.0);
    oper.data.process(3.0);
    oper.data.process(9.0);
    oper.endWindow();
    oper.teardown();

    Assert.assertEquals("Must be one tuple in sink", variance.collectedTuples.size(), 1);
    Assert.assertEquals("Must be one tuple in sink", deviation.collectedTuples.size(), 1);
    Assert.assertEquals("Mismatch in variance  ", new Double(13.333),(Double)variance.collectedTuples.get(0), 0.001);
    Assert.assertEquals("Mismatch in deviation ", new Double(3.6514), (Double)deviation.collectedTuples.get(0), 0.001);
  }
}
