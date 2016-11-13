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

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.window.Tuple;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Sink;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 * A wrapper operator that intercept the tuples and convert them between {@link Tuple}
 *
 * @since 3.5.0
 */
public class TupleWrapperOperator implements InputOperator, Operator.CheckpointNotificationListener
{

  public static class OutputPortWrapper extends DefaultOutputPort implements Sink
  {

    @Override
    public void put(Object o)
    {
      emit(o);
    }

    @Override
    public int getCount(boolean b)
    {
      // No Accumulation
      return 0;
    }
  }

  public static class InputPortWrapper extends DefaultInputPort<Tuple>
  {

    @NotNull
    private DefaultInputPort input;

    public void setInput(DefaultInputPort input)
    {
      this.input = input;
    }

    @Override
    public void process(Tuple o)
    {
      input.process(o.getValue());
    }

    @Override
    public Sink getSink()
    {
      return input.getSink();
    }

    @Override
    public void setConnected(boolean connected)
    {
      input.setConnected(connected);
    }

    @Override
    public void setup(Context.PortContext context)
    {
      input.setup(context);
    }

    @Override
    public void teardown()
    {
      input.teardown();
    }
  }

  @InputPortFieldAnnotation(optional = true)
  public final transient OutputPortWrapper output1 = new OutputPortWrapper();

  @InputPortFieldAnnotation(optional = true)
  public final transient OutputPortWrapper output2 = new OutputPortWrapper();

  @InputPortFieldAnnotation(optional = true)
  public final transient OutputPortWrapper output3 = new OutputPortWrapper();

  @InputPortFieldAnnotation(optional = true)
  public final transient OutputPortWrapper output4 = new OutputPortWrapper();

  @InputPortFieldAnnotation(optional = true)
  public final transient OutputPortWrapper output5 = new OutputPortWrapper();

  @InputPortFieldAnnotation(optional = true)
  public final transient InputPortWrapper input1 = new InputPortWrapper();

  @InputPortFieldAnnotation(optional = true)
  public final transient InputPortWrapper input2 = new InputPortWrapper();

  @InputPortFieldAnnotation(optional = true)
  public final transient InputPortWrapper input3 = new InputPortWrapper();

  @InputPortFieldAnnotation(optional = true)
  public final transient InputPortWrapper input4 = new InputPortWrapper();

  @InputPortFieldAnnotation(optional = true)
  public final transient InputPortWrapper input5 = new InputPortWrapper();

  //delegate to
  @NotNull
  private Operator operator;

  public void setOperator(Operator operator)
  {
    this.operator = operator;
  }

  @Override
  public void beginWindow(long l)
  {
    operator.beginWindow(l);
  }

  @Override
  public void endWindow()
  {
    operator.endWindow();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    operator.endWindow();
  }

  @Override
  public void teardown()
  {
    operator.teardown();
  }

  @Override
  public void beforeCheckpoint(long l)
  {
    if (operator instanceof CheckpointNotificationListener) {
      ((CheckpointNotificationListener)operator).beforeCheckpoint(l);
    }
  }

  @Override
  public void checkpointed(long l)
  {
    if (operator instanceof CheckpointNotificationListener) {
      ((CheckpointNotificationListener)operator).checkpointed(l);
    }
  }

  @Override
  public void committed(long l)
  {
    if (operator instanceof CheckpointNotificationListener) {
      ((CheckpointNotificationListener)operator).committed(l);
    }
  }

  @Override
  public void emitTuples()
  {
    if (operator instanceof InputOperator) {
      ((InputOperator)operator).emitTuples();
    }
  }
}
