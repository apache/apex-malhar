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
package org.apache.apex.malhar.stream.sample;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * A helper class for assertion of results collected
 */
public class TupleCollector<T> extends BaseOperator
{

  public static volatile Map<String, List<?>> results = new HashMap<>();

  public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<>(this);

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<>();

  public String id = "";

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    TupleCollector ownerOperator;

    List<T> list;

    public CollectorInputPort(TupleCollector ownerOperator)
    {
      super();
      this.ownerOperator = ownerOperator;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(T tuple)
    {
      list.add(tuple);
      ownerOperator.outputPort.emit(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        results.put(ownerOperator.id, list = new LinkedList<>());
      }
    }
  }







}
