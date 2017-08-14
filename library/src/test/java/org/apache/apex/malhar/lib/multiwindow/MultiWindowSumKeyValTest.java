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
package org.apache.apex.malhar.lib.multiwindow;


import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * Functional tests for {@link org.apache.apex.malhar.lib.multiwindow.AbstractSlidingWindow}.
 */
public class MultiWindowSumKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(MultiWindowSumKeyValTest.class);
  /**
   * Test functional logic
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws InterruptedException
  {
    MultiWindowSumKeyVal<String, Integer> oper = new MultiWindowSumKeyVal<String, Integer>();

    CollectorTestSink swinSink = new CollectorTestSink();
    oper.sum.setSink(swinSink);

    oper.beginWindow(0);
    KeyValPair<String, Integer> low = new KeyValPair<String, Integer>("a", 3);
    oper.data.process(low);
    KeyValPair<String, Integer> high = new KeyValPair<String, Integer>("a", 11);
    oper.data.process(high);
    oper.endWindow();

    oper.beginWindow(1);
    low = new KeyValPair<String, Integer>("a", 1);
    oper.data.process(low);
    high = new KeyValPair<String, Integer>("a", 9);
    oper.data.process(high);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, swinSink.collectedTuples.size());
    for (Object o : swinSink.collectedTuples) {
      log.debug(o.toString());
    }
  }
}
