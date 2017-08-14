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
package org.apache.apex.malhar.lib.filter;

import java.util.Random;
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
 * Application Test for Filter Operator.
 */
public class FilterAppTest
{
  @Test
  public void testFilterApplication() throws Exception
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
      FilterOperator filter = dag.addOperator("Filter", new FilterOperator());

      filter.setCondition("(({$}.getNum() % 10) == 0)");

      ConsoleOutputOperator trueConsole = dag.addOperator("TrueConsole", new ConsoleOutputOperator());
      trueConsole.setSilent(true);
      ConsoleOutputOperator falseConsole = dag.addOperator("FalseConsole", new ConsoleOutputOperator());
      falseConsole.setSilent(true);
      ConsoleOutputOperator errorConsole = dag.addOperator("ErrorConsole", new ConsoleOutputOperator());
      errorConsole.setSilent(true);

      dag.getMeta(filter).getMeta(filter.input).getAttributes().put(Context.PortContext.TUPLE_CLASS, DummyPOJO.class);

      dag.addStream("Connect", input.output, filter.input);

      dag.addStream("ConditionTrue", filter.truePort, trueConsole.input);
      dag.addStream("ConditionFalse", filter.falsePort, falseConsole.input);
      dag.addStream("ConditionError", filter.error, errorConsole.input);
    }
  }

  public static class DummyPOJO
  {
    private int num;

    public DummyPOJO()
    {
      //for kryo
    }

    public DummyPOJO(int num)
    {
      this.num = num;
    }

    public int getNum()
    {
      return num;
    }

    public void setNum(int num)
    {
      this.num = num;
    }
  }

  public static class DummyInputGenerator implements InputOperator
  {
    public final transient DefaultOutputPort<DummyPOJO> output = new DefaultOutputPort<>();
    Random randomGenerator = new Random();

    @Override
    public void emitTuples()
    {
      output.emit(new DummyPOJO(randomGenerator.nextInt(1000)));
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
