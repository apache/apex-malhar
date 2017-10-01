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
package com.datatorrent.lib.io.fs;

import java.util.List;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class InMemoryDataInputOperator<T> implements InputOperator
{
  private List<T> inputData = null;
  private boolean emissionCompleted = false;
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  public InMemoryDataInputOperator()
  {
    inputData = null;
  }

  public InMemoryDataInputOperator(List<T> data)
  {
    inputData = data;
  }

  @Override
  public void emitTuples()
  {
    if (emissionCompleted) {
      return;
    }
    for (T data : inputData) {
      outputPort.emit(data);
    }
    emissionCompleted = true;
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
