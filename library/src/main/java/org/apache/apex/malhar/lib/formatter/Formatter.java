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
package org.apache.apex.malhar.lib.formatter;

import org.apache.apex.malhar.lib.converter.Converter;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * Abstract class that implements Converter interface. This is a schema enabled
 * Formatter <br>
 * Sub classes need to implement the convert method <br>
 * <b>Port Interface</b><br>
 * <b>in</b>: expects &lt;Object&gt; this is a schema enabled port<br>
 * <b>out</b>: emits &lt;OUTPUT&gt; <br>
 * <b>err</b>: emits &lt;Object&gt; error port that emits input tuple that could
 * not be converted<br>
 * <br>
 *
 * @displayName Parser
 * @tags parser converter
 * @param <OUTPUT>
 * @since 3.2.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class Formatter<OUTPUT> extends BaseOperator implements Converter<Object, OUTPUT>
{
  protected transient Class<?> clazz;

  @AutoMetric
  private long errorTupleCount;
  @AutoMetric
  private long emittedObjectCount;
  @AutoMetric
  private long incomingTuplesCount;

  @OutputPortFieldAnnotation
  public transient DefaultOutputPort<OUTPUT> out = new DefaultOutputPort<OUTPUT>();

  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort<Object> err = new DefaultOutputPort<Object>();

  @InputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    public void setup(PortContext context)
    {
      clazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object inputTuple)
    {
      incomingTuplesCount++;
      OUTPUT tuple = convert(inputTuple);
      if (tuple == null && err.isConnected()) {
        errorTupleCount++;
        err.emit(inputTuple);
        return;
      }
      if (out.isConnected()) {
        emittedObjectCount++;
        out.emit(tuple);
      }
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    errorTupleCount = 0;
    emittedObjectCount = 0;
    incomingTuplesCount = 0;
  }

  /**
   * Get the class that needs to be formatted
   *
   * @return Class<?>
   */
  public Class<?> getClazz()
  {
    return clazz;
  }

  /**
   * Set the class of tuple that needs to be formatted
   *
   * @param clazz
   */
  public void setClazz(Class<?> clazz)
  {
    this.clazz = clazz;
  }

  @VisibleForTesting
  protected long getErrorTupleCount()
  {
    return errorTupleCount;
  }

  @VisibleForTesting
  protected long getEmittedObjectCount()
  {
    return emittedObjectCount;
  }

  @VisibleForTesting
  protected long getIncomingTuplesCount()
  {
    return incomingTuplesCount;
  }

}
