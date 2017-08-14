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

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.math.Division}
 * <p>
 *
 */

public class DivisionTest
{
  /**
   * Test operator logic emits correct results.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testNodeProcessing()
  {
    Division oper = new Division();
    CollectorTestSink lqSink = new CollectorTestSink();
    CollectorTestSink iqSink = new CollectorTestSink();
    CollectorTestSink dqSink = new CollectorTestSink();
    CollectorTestSink fqSink = new CollectorTestSink();
    CollectorTestSink lrSink = new CollectorTestSink();
    CollectorTestSink irSink = new CollectorTestSink();
    CollectorTestSink drSink = new CollectorTestSink();
    CollectorTestSink frSink = new CollectorTestSink();
    CollectorTestSink eSink = new CollectorTestSink();

    oper.longQuotient.setSink(lqSink);
    oper.integerQuotient.setSink(iqSink);
    oper.doubleQuotient.setSink(dqSink);
    oper.floatQuotient.setSink(fqSink);
    oper.longRemainder.setSink(lrSink);
    oper.doubleRemainder.setSink(drSink);
    oper.floatRemainder.setSink(frSink);
    oper.integerRemainder.setSink(irSink);
    oper.errordata.setSink(eSink);

    oper.beginWindow(0); //
    oper.denominator.process(5);
    oper.numerator.process(11);
    oper.denominator.process(0);
    oper.endWindow(); //

    Assert.assertEquals("number emitted tuples", 1,
        lqSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1,
        iqSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1,
        dqSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1,
        fqSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1,
        lrSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1,
        irSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1,
        drSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1,
        frSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 1,
        eSink.collectedTuples.size());

    Assert.assertEquals("quotient is", new Long(2),
        lqSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", 2, iqSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", 2.2, dqSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", new Float(2.2),
        fqSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", new Long(1),
        lrSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", 1, irSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", 1.0, drSink.collectedTuples.get(0));
    Assert.assertEquals("quotient is", new Float(1.0),
        frSink.collectedTuples.get(0));
  }
}
