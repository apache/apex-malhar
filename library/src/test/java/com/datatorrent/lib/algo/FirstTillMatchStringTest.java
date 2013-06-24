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
import com.datatorrent.lib.algo.FirstTillMatchString;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.FirstTillMatchString}<p>
 *
 */
public class FirstTillMatchStringTest
{
  private static Logger log = LoggerFactory.getLogger(FirstTillMatchStringTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings( {"SleepWhileInLoop", "unchecked", "unchecked"})
  public void testNodeProcessing() throws Exception
  {
    FirstTillMatchString<String> oper = new FirstTillMatchString<String>();
    TestSink matchSink = new TestSink();
    oper.first.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, String> input = new HashMap<String, String>();
    input.put("a", "4");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "2");
    oper.data.process(input);
    input.put("a", "3");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "4");
    input.put("b", "21");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "6");
    input.put("b", "20");
    input.put("c", "5");
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, matchSink.collectedTuples.size());
    int atotal = 0;
    Integer aval;
    for (Object o: matchSink.collectedTuples) {
      aval = Integer.parseInt(((HashMap<String, String>)o).get("a"));
      atotal += aval.intValue();
    }
    Assert.assertEquals("Value of a was ", 6, atotal);
    matchSink.clear();

    oper.beginWindow(0);
    input.clear();
    input.put("a", "2");
    input.put("b", "20");
    input.put("c", "1000");
    oper.data.process(input);
    input.clear();
    input.put("a", "5");
    oper.data.process(input);
    oper.endWindow();
    // There should be no emit as all tuples do not match
    Assert.assertEquals("number emitted tuples", 2, matchSink.collectedTuples.size());
    atotal = 0;
    for (Object o: matchSink.collectedTuples) {
      aval = Integer.parseInt(((HashMap<String, String>)o).get("a"));
      atotal += aval.intValue();
    }
    Assert.assertEquals("Value of a was ", 7, atotal);
  }
}
