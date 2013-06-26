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
 * limitations under the License.
 */
package com.datatorrent.lib.logs;

import com.datatorrent.lib.logs.LineTokenizer;
import com.datatorrent.lib.testbench.HashTestSink;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.logs.LineTokenizer}<p>
 *
 */
public class LineTokenizerTest
{
  private static Logger log = LoggerFactory.getLogger(LineTokenizerTest.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeProcessing()
  {

    LineTokenizer oper = new LineTokenizer();
    HashTestSink tokenSink = new HashTestSink();

    oper.setSplitBy(",");
    oper.tokens.setSink(tokenSink);
    oper.beginWindow(0); //

    String input1 = "a,b,c";
    String input2 = "a";
    String input3 = "";
    int numTuples = 1000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow(); //
    Assert.assertEquals("number emitted tuples", 3, tokenSink.map.size());
    //Assert.assertEquals("number of \"a\"", numTuples * 2, tokenSink.getCount("a"));
    //Assert.assertEquals("number of \"b\"", numTuples, tokenSink.getCount("b"));
    //Assert.assertEquals("number of \"c\"", numTuples, tokenSink.getCount("c"));
  }
}
