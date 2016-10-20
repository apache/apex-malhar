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
package org.apache.apex.malhar.stream.api.impl;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.apex.malhar.stream.api.annotation.ControlPort;
import org.apache.apex.malhar.stream.api.function.Function;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * Unit test to default implementation of ApexStream interface
 */
public class ApexStreamImplTest
{

  @Test
  public void testAddOperator()
  {
    LogicalPlan dag = new LogicalPlan();
    TestOperator<String, Integer> firstOperator = new TestOperator<>();
    TestOperator<Integer, Date> secondOperator = new TestOperator<>();
    new ApexStreamImpl<String>().addOperator(firstOperator, null, firstOperator.output, name("first"))
        .endWith(secondOperator, secondOperator.input, name("second"))
        .with(DAG.Locality.THREAD_LOCAL)
        .with(Context.OperatorContext.AUTO_RECORD, true)
        .with("prop", "TestProp").populateDag(dag);
    Assert.assertTrue(dag.getAllOperators().size() == 2);
    Set<String> opNames = new HashSet<>();
    opNames.add("first");
    opNames.add("second");
    for (LogicalPlan.OperatorMeta operatorMeta : dag.getAllOperators()) {
      Assert.assertTrue(operatorMeta.getOperator() instanceof TestOperator);
      Assert.assertTrue(opNames.contains(operatorMeta.getName()));
      if (operatorMeta.getName().equals("second")) {
        // Assert the Context.OperatorContext.AUTO_RECORD attribute has been set to true for second operator
        Assert.assertTrue(operatorMeta.getAttributes().get(Context.OperatorContext.AUTO_RECORD));
        // Assert the prop has been set to TestProp for second operator
        Assert.assertEquals("TestProp", ((TestOperator)operatorMeta.getOperator()).prop);
      } else {
        // Assert the Context.OperatorContext.AUTO_RECORD attribute keeps as default false for first operator
        Assert.assertNull(operatorMeta.getAttributes().get(Context.OperatorContext.AUTO_RECORD));
        // Assert the prop has not been set for first operator
        Assert.assertNull(((TestOperator)operatorMeta.getOperator()).prop);
      }
    }

    Collection<LogicalPlan.StreamMeta> streams = dag.getAllStreams();
    // Assert there is only one stream
    Assert.assertTrue(streams.size() == 1);
    for (LogicalPlan.StreamMeta stream : streams) {

      // Assert the stream is from first operator to second operator
      Assert.assertEquals("first", stream.getSource().getOperatorMeta().getName());
      Assert.assertTrue(1 == stream.getSinks().size());
      Assert.assertEquals("second", stream.getSinks().get(0).getOperatorWrapper().getName());

      // Assert the stream is thread local
      Assert.assertTrue(stream.getLocality() == DAG.Locality.THREAD_LOCAL);
    }

  }


  @Test
  public void testControlPortConnection()
  {
    LogicalPlan dag = new LogicalPlan();

    OperatorWithControlPorts<String, String> first = new OperatorWithControlPorts<>();
    OperatorWithControlPorts<String, String> second = new OperatorWithControlPorts<>();
    OperatorWithControlPorts<String, String> last = new OperatorWithControlPorts<>();

    new ApexStreamImpl<String>().addOperator(first, null, first.output, name("first"))
        .addOperator(second, second.input, second.output, name("second"))
        .map(new Function.MapFunction<String, String>()
        {
          @Override
          public String f(String input)
          {
            // mock function
            return input;
          }
        }, name("third")).endWith(last, last.input, name("fourth")).populateDag(dag);

    // There are 3 data streams and 2 control streams (The map operator is skipped in this case)
    Assert.assertEquals(5, dag.getAllStreams().size());
    for (LogicalPlan.StreamMeta sm : dag.getAllStreams()){
      if (!sm.getSource().getPortName().equals("controlTupleOutput")) {
        continue;
      }

      if (sm.getSource().getOperatorMeta().getName().equals("first")) {
        Assert.assertEquals(1, sm.getSinks().size());
        Assert.assertEquals("second", sm.getSinks().get(0).getOperatorWrapper().getName());
      } else if (sm.getSource().getOperatorMeta().getName().equals("second")) {
        Assert.assertEquals(1, sm.getSinks().size());
        // third operator is skipped because it has no control tuple port
        Assert.assertEquals("fourth", sm.getSinks().get(0).getOperatorWrapper().getName());
      }
    }

  }


  @Test
  public void testWatermarkInjection()
  {
    LogicalPlan dag = new LogicalPlan();

    OperatorWithControlPorts<String, String> first = new OperatorWithControlPorts<>();
    OperatorWithControlPorts<String, String> second = new OperatorWithControlPorts<>();

    new ApexStreamImpl<String>().addOperator(first, null, first.output, name("first"), new TestWatermarkGenerator<String>())
        .addOperator(second, second.input, second.output, name("second")).populateDag(dag);

    // There are 3 data streams and 2 control streams (The map operator is skipped in this case)
    Assert.assertEquals(3, dag.getAllOperators().size());
    // 2 data stream first -> first_postprocess -> second
    // 1 control stream first_postprocess -> second
    Assert.assertEquals(3, dag.getAllStreams().size());
    Assert.assertTrue(Iterables.any(dag.getAllOperators(), new Predicate<LogicalPlan.OperatorMeta>()
    {
      @Override
      public boolean apply(@Nullable LogicalPlan.OperatorMeta operatorMeta)
      {
        return operatorMeta.getName().endsWith(DagMeta.POST_PROCESS_OPERATOR_SUFFIX);
      }
    }));

  }

  /**
   * A mock operator for test
   * @param <T>
   * @param <O>
   */
  public static class TestOperator<T, O> extends BaseOperator
  {

    private String prop = null;

    public void setProp(String prop)
    {
      this.prop = prop;
    }

    public String getProp()
    {
      return prop;
    }

    public final transient InputPort<T> input = new DefaultInputPort<T>()
    {
      @Override
      public void process(T o)
      {

      }
    };

    public final transient OutputPort<O> output = new DefaultOutputPort<>();

  }

  public static class OperatorWithControlPorts<T, O> extends TestOperator<T, O>
  {
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

    @ControlPort
    public final transient OutputPort<ControlTuple> controlTupleOutputPort = new DefaultOutputPort<>();

    @ControlPort
    public final transient DefaultInputPort<ControlTuple> controlTupleInputOperator = new DefaultInputPort<ControlTuple>()
    {
      @Override
      public void process(ControlTuple o)
      {

      }
    };

  }

  public static class TestWatermarkGenerator<T> extends Option.WatermarkGenerator<T>
  {

    @Override
    public ControlTuple.Watermark currentWatermark()
    {
      return null;
    }

    @Override
    public ControlTuple.Watermark getWatermarkFromTuple(T input)
    {
      return null;
    }
  }

}
