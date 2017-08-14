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
package org.apache.apex.malhar.lib.transform;


import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.stram.engine.PortContext;

/**
 * Test for Transform Operator.
 */
public class TransformOperatorTest
{
  private TransformOperator operator;
  CollectorTestSink<Object> sink;

  @Rule
  public TestUtils.TestInfo testMeta = new TestUtils.TestInfo()
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      operator = new TransformOperator();

      sink = new CollectorTestSink<>();
      TestUtils.setSink(operator.output, sink);

      operator.setup(null);

      Attribute.AttributeMap inMap = new Attribute.AttributeMap.DefaultAttributeMap();
      inMap.put(Context.PortContext.TUPLE_CLASS, InputClass.class);
      operator.input.setup(new PortContext(inMap, null));

      Attribute.AttributeMap outMap = new Attribute.AttributeMap.DefaultAttributeMap();
      outMap.put(Context.PortContext.TUPLE_CLASS, OutputClass.class);
      operator.output.setup(new PortContext(outMap, null));
    }

    @Override
    protected void finished(Description description)
    {
      operator.deactivate();
      operator.teardown();
      super.finished(description);
    }
  };

  @Test
  public void testTransformOperator()
  {
    // Set expression map
    Map<String, String> expMap = new HashMap<>();
    expMap.put("var21", "var1");
    expMap.put("var22", "$.var2");
    expMap.put("var23", "{$.var3}");
    operator.setExpressionMap(expMap);
    operator.activate(null);

    operator.beginWindow(1L);
    InputClass inputClass = new InputClass();
    inputClass.setVar1(123);
    inputClass.setVar2(12);
    inputClass.setVar3("ABC");
    operator.input.put(inputClass);
    operator.endWindow();

    Assert.assertEquals(1, sink.collectedTuples.size());
    Object o = sink.collectedTuples.get(0);
    Assert.assertTrue(o instanceof OutputClass);
    OutputClass out = (OutputClass)o;
    Assert.assertEquals(123, out.getVar21());
    Assert.assertEquals(12, out.getVar22());
    Assert.assertEquals("ABC", out.getVar23());
  }

  @Test
  public void testComplexTransformOperator()
  {
    // Set expression map
    Map<String, String> expMap = new HashMap<>();
    expMap.put("var21", "{$.var1} * 123");
    expMap.put("var22", "round(pow({$.var2}, {$.var1}/50))");
    expMap.put("var23", "{$.var3}.toLowerCase()");
    operator.setExpressionMap(expMap);
    operator.activate(null);

    operator.beginWindow(1L);
    InputClass inputClass = new InputClass();
    inputClass.setVar1(123);
    inputClass.setVar2(12);
    inputClass.setVar3("ABC");
    operator.input.put(inputClass);
    operator.endWindow();

    Assert.assertEquals(1, sink.collectedTuples.size());
    Object o = sink.collectedTuples.get(0);
    Assert.assertTrue(o instanceof OutputClass);
    OutputClass out = (OutputClass)o;
    Assert.assertEquals(15129, out.getVar21());
    Assert.assertEquals(144, out.getVar22());
    Assert.assertEquals("abc", out.getVar23());
  }

  @Test
  public void testCopyFieldOperator()
  {
    operator.setCopyMatchingFields(true);

    // Set expression map
    Map<String, String> expMap = new HashMap<>();
    expMap.put("var21", "{$.var1} * 123");
    expMap.put("var22", "round(pow({$.var2}, {$.var1}/50))");
    expMap.put("var23", "{$.var3}.toLowerCase()");
    expMap.put("var4", "{$.var4}.toLowerCase()");
    operator.setExpressionMap(expMap);
    operator.activate(null);

    operator.beginWindow(1L);
    InputClass inputClass = new InputClass();
    inputClass.setVar1(123);
    inputClass.setVar2(12);
    inputClass.setVar3("ABC");
    inputClass.setVar4("XYZ");
    inputClass.setVar5(12345);
    inputClass.setVar6(123);
    inputClass.setVar7(456);
    inputClass.setVar9(789);
    operator.input.put(inputClass);
    operator.endWindow();

    Assert.assertEquals(1, sink.collectedTuples.size());
    Object o = sink.collectedTuples.get(0);
    Assert.assertTrue(o instanceof OutputClass);
    OutputClass out = (OutputClass)o;
    Assert.assertEquals(15129, out.getVar21());
    Assert.assertEquals(144, out.getVar22());
    Assert.assertEquals("abc", out.getVar23());
    Assert.assertEquals("xyz", out.getVar4());
    Assert.assertEquals(12345, out.getVar5());
    Assert.assertEquals(123, out.getVar6());
    Assert.assertEquals(0, out.getVar7());
    Assert.assertEquals(0, out.getVar8());
  }

  public static class InputClass
  {
    private int var1;
    public long var2;
    private String var3;
    private String var4;
    public int var5;
    private long var6;
    public int var7;
    public long var9;

    public int getVar1()
    {
      return var1;
    }

    public void setVar1(int var1)
    {
      this.var1 = var1;
    }

    public long getVar2()
    {
      return var2;
    }

    public void setVar2(long var2)
    {
      this.var2 = var2;
    }

    public String getVar3()
    {
      return var3;
    }

    public void setVar3(String var3)
    {
      this.var3 = var3;
    }

    public String getVar4()
    {
      return var4;
    }

    public void setVar4(String var4)
    {
      this.var4 = var4;
    }

    public int getVar5()
    {
      return var5;
    }

    public void setVar5(int var5)
    {
      this.var5 = var5;
    }

    public long getVar6()
    {
      return var6;
    }

    public void setVar6(long var6)
    {
      this.var6 = var6;
    }

    public int getVar7()
    {
      return var7;
    }

    public void setVar7(int var7)
    {
      this.var7 = var7;
    }

    public long getVar9()
    {
      return var9;
    }

    public void setVar9(long var9)
    {
      this.var9 = var9;
    }
  }

  public static class OutputClass
  {
    private int var21;
    public long var22;
    private String var23;
    private String var4;
    public int var5;
    public long var6;
    public long var7;
    private long var8;

    public int getVar21()
    {
      return var21;
    }

    public void setVar21(int var21)
    {
      this.var21 = var21;
    }

    public long getVar22()
    {
      return var22;
    }

    public void setVar22(long var22)
    {
      this.var22 = var22;
    }

    public String getVar23()
    {
      return var23;
    }

    public void setVar23(String var23)
    {
      this.var23 = var23;
    }

    public String getVar4()
    {
      return var4;
    }

    public void setVar4(String var4)
    {
      this.var4 = var4;
    }

    public int getVar5()
    {
      return var5;
    }

    public void setVar5(int var5)
    {
      this.var5 = var5;
    }

    public long getVar6()
    {
      return var6;
    }

    public void setVar6(long var6)
    {
      this.var6 = var6;
    }

    public long getVar7()
    {
      return var7;
    }

    public void setVar7(long var7)
    {
      this.var7 = var7;
    }

    public long getVar8()
    {
      return var8;
    }

    public void setVar8(long var8)
    {
      this.var8 = var8;
    }
  }
}
