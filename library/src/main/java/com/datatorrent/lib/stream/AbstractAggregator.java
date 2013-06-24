/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.OperatorContext;

import java.util.Collection;
import javax.validation.constraints.Min;

/**
 *
 * @param <T>
 */
public abstract class AbstractAggregator<T> implements Operator
{
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      if (collection == null) {
        collection = getNewCollection(size);
      }

      collection.add(tuple);

      if (collection.size() == size) {
        output.emit(collection);
        collection = null;
      }
    }

  };
  public final transient DefaultOutputPort<Collection<T>> output = new DefaultOutputPort<Collection<T>>();

  /**
   * Set the size of the collection.
   *
   * If set to zero, the collection collects all the tuples within a window and
   * emits the collection as 1 output tuple at the end of the window.
   * If set to positive value, it collects the collection as soon as the size
   * of the collection reaches the size.
   *
   * @param size the size to set
   */
  public void setSize(int size)
  {
    this.size = size;
  }

  @Min(0)
  public int getSize()
  {
    return size;
  }

  public abstract Collection<T> getNewCollection(int size);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    if (size == 0 && collection != null) {
      output.emit(collection);
      collection = null;
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  protected Collection<T> collection;
  @Min(0)
  private int size = 0;
}
