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
import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

/**
 * Application Test for Transform Operator.
 */
public class TransformOperatorAppTest
{
  @Test
  public void testExpressionApplication() throws Exception
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
    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      DummyInputGenerator input = dag.addOperator("Input", new DummyInputGenerator());
      TransformOperator transform = dag.addOperator("Transform", new TransformOperator());

      Map<String, String> expMap = new HashMap<>();
      expMap.put("firstname", "lastname");
      expMap.put("lastname", "firstname");
      transform.setExpressionMap(expMap);

      ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());
      console.setSilent(true);
      dag.getMeta(transform).getMeta(transform.output).getAttributes().put(Context.PortContext.TUPLE_CLASS, TestPojo.class);
      dag.getMeta(transform).getMeta(transform.input).getAttributes().put(Context.PortContext.TUPLE_CLASS, TestPojo.class);

      dag.addStream("Connect", input.output, transform.input);
      dag.addStream("Ppint", transform.output, console.input);
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

    @Override
    public void emitTuples()
    {
      output.emit(new TestPojo("FirstName", "LastName"));
    }

    @Override
    public void beginWindow(long l)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }
  }
}
