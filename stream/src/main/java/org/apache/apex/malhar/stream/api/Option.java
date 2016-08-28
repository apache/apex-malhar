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

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Attribute;

/**
 * Options for the operators in the dag
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface Option
{

  class Options
  {
    public static Option name(String name)
    {
      return new OpName(name);
    }

    public static Option prop(String name, Object value)
    {
      return new PropSetting(name, value);
    }

    public static <T> Option attr(Attribute<T> attr, T obj)
    {
      return new AttributeSetting<>(attr, obj);
    }
  }

  /**
   * An option used to set the name of the operator
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
   * An option used to set the property value of the operator
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
   * An option used to set the {@link Attribute}
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
