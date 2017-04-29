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
package org.apache.apex.examples.pi;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>An operator which converts a raw value to a named value singleton list.</p>
 * AppDataSnapshotServerMap.input accepts a List<Map<String,Object>> so we use this operator to
 * convert individual values to a singleton list of a named value
 * <p>
 * @displayNamed Value
 * @tags count
 * @since 3.2.0
 */
public class NamedValueList<T> extends BaseOperator
{
  @NotNull
  private String valueName;

  private List<Map<String, T>> valueList;
  private Map<String, T> valueMap;

  public final transient DefaultInputPort<T> inPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T val)
    {
      valueMap.put(valueName, val);
      outPort.emit(valueList);
    }
  };

  public final transient DefaultOutputPort<List<Map<String, T>>> outPort = new DefaultOutputPort<>();

  @Override
  public void setup(OperatorContext context)
  {
    valueMap = new HashMap<>();
    valueMap.put(valueName, null);
    valueList = Collections.singletonList(valueMap);
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  public String getValueName()
  {
    return valueName;
  }

  public void setValueName(String name)
  {
    valueName = name;
  }
}
