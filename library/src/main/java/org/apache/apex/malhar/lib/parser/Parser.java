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
package org.apache.apex.malhar.lib.parser;

import org.apache.apex.malhar.lib.converter.Converter;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * Abstract class that implements Converter interface. This is a schema enabled
 * Parser <br>
 * Sub classes need to implement the convert method <br>
 * <br>
 * <b>Port Interface</b><br>
 * <b>in</b>: expects &lt;INPUT&gt;<br>
 * <b>out</b>: emits &lt;Object&gt; this is a schema enabled port<br>
 * <b>err</b>: emits &lt;INPUT&gt; error port that emits input tuple that could
 * not be converted<br>
 * <br>
 *
 * @displayName Parser
 * @tags parser converter
 * @param <INPUT>
 * @since 3.2.0
 */
@InterfaceStability.Evolving
public abstract class Parser<INPUT, ERROROUT> extends BaseOperator implements Converter<INPUT, Object>
{
  protected transient Class<?> clazz;
  @AutoMetric
  protected long errorTupleCount;
  @AutoMetric
  protected long emittedObjectCount;
  @AutoMetric
  protected long incomingTuplesCount;

  @OutputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultOutputPort<Object> out = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      clazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  public transient DefaultOutputPort<ERROROUT> err = new DefaultOutputPort<ERROROUT>();
  public transient DefaultInputPort<INPUT> in = new DefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT inputTuple)
    {
      incomingTuplesCount++;
      processTuple(inputTuple);
    }
  };

  public void processTuple(INPUT inputTuple)
  {
    Object tuple = convert(inputTuple);
    if (tuple == null && err.isConnected()) {
      errorTupleCount++;
      err.emit(processErrorTuple(inputTuple));
      return;
    }
    if (out.isConnected()) {
      emittedObjectCount++;
      out.emit(tuple);
    }
  }

  public abstract ERROROUT processErrorTuple(INPUT input);

  @Override
  public void beginWindow(long windowId)
  {
    errorTupleCount = 0;
    emittedObjectCount = 0;
    incomingTuplesCount = 0;
  }

  @Override
  public void endWindow()
  {
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
}
