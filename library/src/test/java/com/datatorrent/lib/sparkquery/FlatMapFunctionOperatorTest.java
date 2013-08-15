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
package com.datatorrent.lib.sparkquery;

import org.junit.Test;

import com.datatorrent.lib.sparkquery.mapfunction.StringSplitCall;
import com.datatorrent.lib.testbench.CollectorTestSink;


/**
 * Functional test for {@link com.datatorrent.lib.sparkquery.FlatMapFunctionOperator}.
 */
public class FlatMapFunctionOperatorTest
{
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void test()
  {
    FlatMapFunctionOperator<String, String> oper = new FlatMapFunctionOperator<String, String>();
    oper.setCall(new StringSplitCall());
    CollectorTestSink sink = new CollectorTestSink();
    oper.outport.setSink(sink);
    
    oper.beginWindow(1);
    oper.inport.process("test is test");
    oper.inport.process("test is test");
    oper.inport.process("test is test");
    oper.endWindow();
    
    System.out.println(sink.collectedTuples.toString());
  }
}
