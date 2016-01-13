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
package com.datatorrent.lib.expressions;

import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class ExpressionEvaluatorTest
{
  @Test
  public void testBasic() throws Exception
  {
    POJO1 pojo = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    // Operand type: ${objectPlaceholder.fieldName}
    String expression = "${inp.a}";
    ExpressionEvaluator.Expression<Integer> getter = ee.createExecutableExpression(expression, Integer.class);
    Integer age = getter.execute(pojo);
    Assert.assertEquals(12, age.intValue());

    // Operand type: ${fieldName}
    expression = "${a}";
    getter = ee.createExecutableExpression(expression, Integer.class);
    age = getter.execute(pojo);
    Assert.assertEquals(12, age.intValue());

    // Operand type: ${fieldName} with multiple registered placeholders.
    ee.setInputObjectPlaceholders(new String[]{"inp", "inpA"}, new Class[]{POJO1.class, POJO1.class});
    expression = "${a}";
    getter = ee.createExecutableExpression(expression, Integer.class);
    age = getter.execute(pojo, pojo);
    Assert.assertEquals(12, age.intValue());

    // Operand type: ${objectPlaceholder}
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{Integer.class});
    expression = "${inp} * 2";
    getter = ee.createExecutableExpression(expression, Integer.class);
    age = getter.execute(new Integer(6));
    Assert.assertEquals(12, age.intValue());

    // Operand type: ${objectPlaceholder} with placeholder same as variable name
    ee.setInputObjectPlaceholders(new String[]{"a"}, new Class[]{Integer.class});
    expression = "${a} * 2";
    getter = ee.createExecutableExpression(expression, Integer.class);
    age = getter.execute(new Integer(66));
    Assert.assertEquals(132, age.intValue());

    // Operand type : Nested Operands ${objectPlaceholder.fieldName1.fieldName2}
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{NestedPOJO.class});
    expression = "${inp.innerPOJO.a} + toInt(${inp.innerPOJO.b}) + ${inp.innerPOJO2.a} + toInt(${inp.innerPOJO2.b})";
    getter = ee.createExecutableExpression(expression, Integer.class);

    NestedPOJO nestedPOJO = new NestedPOJO();
    nestedPOJO.setInnerPOJO(createTestPOJO1());
    nestedPOJO.innerPOJO2 = createTestPOJO1();
    age = getter.execute(nestedPOJO);
    Assert.assertEquals(50, age.intValue());
  }

  @Test
  public void testSerialization() throws Exception
  {
    POJO1 pojo = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression = "${inp.a} + ${inp.b}";
    ExpressionEvaluator.Expression<Long> getter = ee.createExecutableExpression(expression, Long.class);
    Long addition = getter.execute(pojo);
    Assert.assertEquals(25, addition.longValue());

    TestUtils.clone(new Kryo(), getter);
    TestUtils.clone(new Kryo(), ee);
  }

  @Test
  public void testAddition() throws Exception
  {
    POJO1 pojo = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression = "${inp.a} + ${inp.b}";
    ExpressionEvaluator.Expression<Long> getter = ee.createExecutableExpression(expression, Long.class);
    Long addition = getter.execute(pojo);
    Assert.assertEquals(25, addition.longValue());
  }


  @Test
  public void testReuse() throws Exception
  {
    POJO1 pojo = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression1 = "${inp.a} + ${inp.b}";
    ExpressionEvaluator.Expression<Long> getter1 = ee.createExecutableExpression(expression1, Long.class);
    Long addition = getter1.execute(pojo);
    Assert.assertEquals(25, addition.longValue());
  }

  @Test
  public void testMultiplication() throws Exception
  {
    POJO1 pojo = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression = "${inp.a} * ${inp.b}";
    ExpressionEvaluator.Expression<Long> getter = ee.createExecutableExpression(expression, Long.class);
    Long mult = getter.execute(pojo);
    Assert.assertEquals(156, mult.longValue());
  }

  @Test
  public void testCasting() throws Exception
  {
    POJO1 pojo = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression = "(int) (${inp.a} * ${inp.b})";
    ExpressionEvaluator.Expression<Integer> getter = ee.createExecutableExpression(expression, Integer.class);
    Integer mult = getter.execute(pojo);
    Assert.assertEquals(156, mult.intValue());
  }

  @Test
  public void testCondition() throws Exception
  {
    POJO1 pojo = createTestPOJO1();
    pojo.setA(1);
    pojo.setB(1);

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression = "(${inp.a} * ${inp.b}) > 0 ? ${inp.name1} : ${inp.name2}";
    ExpressionEvaluator.Expression<String> getter = ee.createExecutableExpression(expression, String.class);

    String result = getter.execute(pojo);
    Assert.assertEquals("Apex", result);

    pojo.setA(-1);
    result = getter.execute(pojo);
    Assert.assertEquals("DataTorrent", result);

    pojo.setB(-1);
    result = getter.execute(pojo);
    Assert.assertEquals("Apex", result);
  }

  @Test
  public void testStringConcat() throws Exception
  {
    POJO1 pojo = createTestPOJO1();
    pojo.setA(1);
    pojo.setB(1);

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression = "${inp.name2} + \" \" + ${inp.name1}";
    ExpressionEvaluator.Expression<String> getter = ee.createExecutableExpression(expression, String.class);

    String result = getter.execute(pojo);
    Assert.assertEquals("DataTorrent Apex", result);
  }

  @Test
  public void testStringEmptyCheck() throws Exception
  {
    POJO1 pojo = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression = "isEmpty(${inp.name1})";
    ExpressionEvaluator.Expression<Boolean> getter = ee.createExecutableExpression(expression, Boolean.class);

    Boolean result = getter.execute(pojo);
    Assert.assertEquals(false, result);

    pojo.name1 = "";
    result = getter.execute(pojo);
    Assert.assertEquals(true, result);

    pojo.name1 = null;
    result = getter.execute(pojo);
    Assert.assertEquals(true, result);

    pojo.name1 = "Apex";
    String expression1 = "isEmpty(${inp.name1}) ? \"True\" : \"False\"";
    ExpressionEvaluator.Expression<String> getter1 = ee.createExecutableExpression(expression1, String.class);
    String result1 = getter1.execute(pojo);
    Assert.assertEquals("False", result1);

    pojo.name1 = null;
    result1 = getter1.execute(pojo);
    Assert.assertEquals("True", result1);
  }

  @Test
  public void testStringEqualsCheck() throws Exception
  {
    POJO1 pojo = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression = "equalsWithCase(${inp.name1}, ${inp.name2}) ? true: false";
    ExpressionEvaluator.Expression<Boolean> getter = ee.createExecutableExpression(expression, Boolean.class);

    Boolean result = getter.execute(pojo);
    Assert.assertEquals(false, result);

    pojo.name1 = "Datatorrent";
    result = getter.execute(pojo);
    Assert.assertEquals(false, result);

    pojo.name1 = "DataTorrent";
    result = getter.execute(pojo);
    Assert.assertEquals(true, result);
  }

  @Test
  public void testMultiplePOJO() throws Exception
  {
    POJO1 pojo1 = createTestPOJO1();
    POJO2 pojo2 = createTestPOJO2();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inpA", "inpB"}, new Class[]{POJO1.class, POJO2.class});

    String expression = "equalsWithCase(${inpA.name1}, ${inpA.name2}) ? ${inpB.a} : ${inpB.b}";
    ExpressionEvaluator.Expression<Double> getter = ee.createExecutableExpression(expression, Double.class);

    Double result = getter.execute(pojo1, pojo2);
    Assert.assertEquals(pojo2.getB(), result.doubleValue(), 0.0);

    pojo1.name1 = "DataTorrent";
    result = getter.execute(pojo1, pojo2);
    Assert.assertEquals(pojo2.getA(), result.doubleValue(), 0.0);

    String expression1 = "equalsWithCase(${inpA.name1}, ${inpA.name2}) ? ${inpB.a} : toInt(${inpB.b})";
    ExpressionEvaluator.Expression<Integer> getter1 = ee.createExecutableExpression(expression1, Integer.class);

    Integer result1 = getter1.execute(pojo1, pojo2);
    Assert.assertEquals(pojo2.getA(), result1.intValue());

    pojo1.name1 = "DataTorrent";
    result1 = getter1.execute(pojo1, pojo2);
    System.out.println(result1);
    Assert.assertEquals(new Double(pojo2.getB()).intValue(), result1.intValue());
  }

  @Test
  public void testCompleteFunction() throws Exception
  {
    POJO1 pojo = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    // Let expression evaluator know what are the object mappings present in expressions and their class types.
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{POJO1.class});

    String expression = "long retVal = 1; for (int i=0;i<${inp.a};i++) { retVal = retVal * ${inp.b}; } return retVal;";
    ExpressionEvaluator.Expression<Long> getter = ee.createExecutableFunction(expression, Long.class);

    Long result = getter.execute(pojo);
    Assert.assertEquals(new Double(Math.pow(pojo.b, pojo.getA())).longValue(), result.longValue());
  }

  @Test
  public void testAddCustomMethod()
  {
    NestedPOJO nestedPOJO = new NestedPOJO();
    nestedPOJO.setInnerPOJO(createTestPOJO1());
    nestedPOJO.innerPOJO2 = createTestPOJO1();

    ExpressionEvaluator ee = new ExpressionEvaluator();
    ee.setInputObjectPlaceholders(new String[]{"inp"}, new Class[]{NestedPOJO.class});
    ee.registerCustomMethod("com.datatorrent.lib.expressions.ExpressionEvaluatorTest.customMethod");

    String expression = "${inp.innerPOJO.name1} + customMethod(${inp})";
    ExpressionEvaluator.Expression exp = ee.createExecutableExpression(expression, String.class);
    Object result = exp.execute(nestedPOJO);
    Assert.assertTrue(result instanceof String);
    String result1 = (String)result;
    Assert.assertEquals("Apex25", result1);
  }

  public static long customMethod(NestedPOJO pojo)
  {
    return pojo.getInnerPOJO().getA() + pojo.innerPOJO2.b;
  }

  private POJO1 createTestPOJO1()
  {
    POJO1 pojo = new POJO1();
    pojo.setA(12);
    pojo.setB(13);
    pojo.setD(new Date(1988 - 1900, 2, 11));
    pojo.name1 = "Apex";
    pojo.name2 = "DataTorrent";

    return pojo;
  }

  private POJO2 createTestPOJO2()
  {
    POJO2 pojo = new POJO2();
    pojo.setA(1234);
    pojo.setB(1234.56D);
    return pojo;
  }

  public static class POJO1
  {
    private int a;
    public long b;
    private Date d;
    public String name1;
    public String name2;

    public int getA()
    {
      return a;
    }

    public void setA(int a)
    {
      this.a = a;
    }

    public long getB()
    {
      return b;
    }

    public void setB(long b)
    {
      this.b = b;
    }

    public Date getD()
    {
      return d;
    }

    public void setD(Date d)
    {
      this.d = d;
    }
  }

  public static class POJO2
  {
    private int a;
    private double b;

    public int getA()
    {
      return a;
    }

    public void setA(int a)
    {
      this.a = a;
    }

    public double getB()
    {
      return b;
    }

    public void setB(double b)
    {
      this.b = b;
    }
  }

  public static class NestedPOJO
  {
    private POJO1 innerPOJO;
    public POJO1 innerPOJO2;

    public POJO1 getInnerPOJO()
    {
      return innerPOJO;
    }

    public void setInnerPOJO(POJO1 innerPOJO)
    {
      this.innerPOJO = innerPOJO;
    }
  }

}
