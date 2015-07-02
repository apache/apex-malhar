/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.ruby;

import com.datatorrent.lib.testbench.CollectorTestSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * Unit test for RubyOperator.
 *
 */
public class RubyOperatorTest {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void TestRubyOperatorInvoke() {

    RubyOperator oper = new RubyOperator();
    String setupScript = "def square(val)\n";
    setupScript += "  return val*val\nend\n";
    oper.addSetupScript(setupScript);
    oper.setInvoke("square");
    oper.setPassThru(true);

    CollectorTestSink sink = new CollectorTestSink();
    oper.result.setSink(sink);
    HashMap<String, Object> tuple = new HashMap<String, Object>();
    tuple.put("val", 2);
    oper.setup(null);
    oper.beginWindow(0);
    oper.inBindings.process(tuple);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
    for (Object o : sink.collectedTuples) {
      Integer val = Integer.parseInt(o.toString());
      Assert.assertEquals("emitted should be 4", new Integer(4), val);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void TestRubyOperatorEval() {

    RubyOperator oper = new RubyOperator();
    String setupScript1 = "b=0";
    String setupScript2 = "c=0";
    oper.addSetupScript(setupScript1);
    oper.addSetupScript(setupScript2);
    String script = "a = b+c \n d = a*10";
    oper.setEval(script);
    oper.setPassThru(true);
    CollectorTestSink sink = new CollectorTestSink();
    oper.result.setSink(sink);
    HashMap<String, Object> tuple = new HashMap<String, Object>();
    tuple.put("b", 2);
    tuple.put("c", 3);
    oper.setup(null);
    oper.beginWindow(0);
    oper.inBindings.process(tuple);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
    for (Object o : sink.collectedTuples) {
      Integer val = Integer.parseInt(o.toString());
      Assert.assertEquals("emitted should be 50", new Integer(50), val);
    }
  }

}
