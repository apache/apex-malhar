/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.UniqueCounterValue;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.UniqueCounterValue}<p>
 *
 */
public class UniqueCounterValueTest
{
  private static Logger log = LoggerFactory.getLogger(UniqueCounterValueTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    UniqueCounterValue<String> oper = new UniqueCounterValue<String>();
    TestSink sink = new TestSink();
    oper.count.setSink(sink);

    int numTuples = 1000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      oper.data.process("a");
      oper.data.process("b");
      oper.data.process("c");
      oper.data.process("d");
      oper.data.process("e");
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples to TestSink", 1, sink.collectedTuples.size());
    Assert.assertEquals("count emitted tuples", numTuples*5, sink.collectedTuples.get(0));
  }
}
