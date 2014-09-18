/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hbase;

import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import java.io.IOException;

/**
 * Provides the base class for HBase input operators.<br>
 * <p>
 * <br>
 * @displayName: HBase Input
 * @category: store
 * @tag: input
 * @param <T> The tuple type
 * @since 0.3.2
 */
public abstract class HBaseInputOperator<T> extends HBaseOperatorBase implements InputOperator
{

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  //protected abstract T getTuple(Result result);
  //protected abstract T getTuple(KeyValue kv);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    try{
      setupConfiguration();
    } catch (IOException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public void teardown()
  {
  }

}
