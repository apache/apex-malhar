/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.Distinct;
import com.datatorrent.lib.testbench.HashTestSink;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.Distinct<p>
 *
 */
public class DistinctTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings( {"rawtypes", "unchecked"})
  public void testNodeProcessing() throws Exception
  {
    Distinct<String> oper = new Distinct<String>();

    HashTestSink sortSink = new HashTestSink<String>();
    oper.distinct.setSink((HashTestSink<Object>)sortSink);

    oper.beginWindow(0);
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("b");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("b");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("c");
    oper.data.process("a");
    oper.data.process("a");
    oper.data.process("c");
    oper.data.process("d");
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 4, sortSink.size());
    Assert.assertEquals("number of \"a\"", 1, sortSink.getCount("a"));
    Assert.assertEquals("number of \"b\"", 1, sortSink.getCount("b"));
    Assert.assertEquals("number of \"c\"", 1, sortSink.getCount("c"));
    Assert.assertEquals("number of \"d\"", 1, sortSink.getCount("d"));
  }
}
