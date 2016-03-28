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

import com.datatorrent.api.*;
import com.datatorrent.common.util.BaseOperator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolationException;

public class JavaExpressionApplicationTest
{
  @Test public void testExpressionApplication() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class Application implements StreamingApplication
  {
    @Override public void populateDAG(DAG dag, Configuration configuration)
    {
      String exp = "upperCase(${" + DummyProcessor.VARIABLE_PLACEHOLDER + ".firstname}) + " +
          "\" \" + " +
          "upperCase(${" + DummyProcessor.VARIABLE_PLACEHOLDER + ".lastname})";

      DummyInputGenerator input = dag.addOperator("Input", new DummyInputGenerator());
      DummyProcessor processor = dag.addOperator("Processor", new DummyProcessor());
      processor.setStringExp(exp);

      dag.addStream("Connect", input.output, processor.input);
    }
  }

  public static class TestPojo
  {
    private String firstname;
    public String lastname;

    public TestPojo()
    {
      //for kryo
    }

    public TestPojo(String firstname, String lastname)
    {
      this.firstname = firstname;
      this.lastname = lastname;
    }

    public String getFirstname()
    {
      return firstname;
    }

    public void setFirstname(String firstname)
    {
      this.firstname = firstname;
    }
  }

  public static class DummyInputGenerator implements InputOperator
  {
    public final transient DefaultOutputPort<TestPojo> output = new DefaultOutputPort<>();

    @Override public void emitTuples()
    {
      output.emit(new TestPojo("FirstName", "LastName"));
    }

    @Override public void beginWindow(long l)
    {
    }

    @Override public void endWindow()
    {
    }

    @Override public void setup(Context.OperatorContext context)
    {
    }

    @Override public void teardown()
    {
    }
  }

  public static class DummyProcessor extends BaseOperator
  {
    public static String VARIABLE_PLACEHOLDER = "inp";

    private String stringExp;
    private JavaExpressionParser parser;
    private JavaExpressionEvaluator ee;
    private ExpressionEvaluator.Expression expression;

    public final transient DefaultInputPort<TestPojo> input = new DefaultInputPort<TestPojo>()
    {
      @Override public void process(TestPojo testPojo)
      {
        Object result = expression.execute(testPojo);
        Assert.assertTrue(result instanceof String);
        String result1 = (String)result;
        Assert.assertEquals("FIRSTNAME LASTNAME", result1);
      }
    };

    @Override public void setup(Context.OperatorContext context)
    {
      parser = new JavaExpressionParser();
      parser.setInputObjectPlaceholders(new String[] {VARIABLE_PLACEHOLDER}, new Class[] {TestPojo.class});
      ee = new JavaExpressionEvaluator();
      ee.setExpressionParser(parser);

      expression = ee.createExecutableExpression(stringExp, String.class);
    }

    public String getStringExp()
    {
      return stringExp;
    }

    public void setStringExp(String stringExp)
    {
      this.stringExp = stringExp;
    }
  }
}
