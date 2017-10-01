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
package org.apache.apex.malhar.stream.api;

import org.apache.apex.malhar.lib.window.ControlTuple;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Component;
import com.datatorrent.api.Context;

/**
 * Options for each transformation in the dag
 */
public interface Option
{
  abstract class WatermarkGenerator<T> implements Option, Component<Context.OperatorContext>
  {

    @Override
    public void setup(Context.OperatorContext context)
    {

    }

    @Override
    public void teardown()
    {

    }

    /**
     * The current watermark generation will be called each endWindow to generate watermark
     * @return null if you don't want to generate watermark for this window
     */
    public abstract ControlTuple.Watermark currentWatermark();

    /**
     * Generate the watermark from current tuple
     * @param input
     * @return null if you can't generate watermark from current tuple
     */
    public abstract ControlTuple.Watermark getWatermarkFromTuple(T input);
  }

  /**
   * Factory class to generate options
   */
  class Options
  {
    /**
     * Create name options so the operator will be create with the name
     * @param name operator name
     * @return
     */
    public static Option name(String name)
    {
      return new OpName(name);
    }

    /**
     * Create property options to set properties for the operator
     * @param name
     * @param value
     * @return
     */
    public static Option prop(String name, Object value)
    {
      return new PropSetting(name, value);
    }

    /**
     * Create attribte options to set the operator attributes
     * @param attr
     * @param obj
     * @param <T>
     * @return
     */
    public static <T> Option attr(Attribute<T> attr, T obj)
    {
      return new AttributeSetting<>(attr, obj);
    }
  }

  /**
   * An option to set the operator name
   */
  class OpName implements Option
  {

    private String name;

    public OpName(String name)
    {
      this.name = name;
    }

    public String getName()
    {
      return name;
    }
  }

  /**
   * An option to set an operator property value
   */
  class PropSetting implements Option
  {

    private String name;

    private Object val;

    public PropSetting(String name, Object val)
    {
      this.name = name;
      this.val = val;
    }

    public String getName()
    {
      return name;
    }

    public Object getVal()
    {
      return val;
    }
  }

  /**
   * An option to set an attribute value
   * @param <T>
   */
  class AttributeSetting<T> implements Option
  {
    private Attribute<T> attr;

    private T value;

    public AttributeSetting(Attribute<T> attr, T value)
    {
      this.attr = attr;
      this.value = value;
    }

    public Attribute<T> getAttr()
    {
      return attr;
    }

    public T getValue()
    {
      return value;
    }
  }
}
