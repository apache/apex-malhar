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
package org.apache.apex.malhar.lib.stream;

import java.util.Collection;

import javax.validation.constraints.Min;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 *
 * A base implementation of an operator that aggregates tuples.&nbsp; Subclasses should provide the
   implementation to get a new collection of a given size.
 * <p>
 * If size of collection is 0 then all tuples till end window are aggregated, otherwise collection is emitted as
 * soon as collection size reaches to given size. <br>
 * <br>
 * <b>StateFull : Yes </b>, values are collected over application window. <br>
 * <b>Partitions : No</b>, will yield wrong results.
 * @displayName Abstract Aggregator
 * @category Stream Manipulators
 * @tags aggregate
 * @param <T>
 *          Aggregate tuple type.
 * @since 0.3.3
 */
@OperatorAnnotation(partitionable = false)
public abstract class AbstractAggregator<T> implements Operator
{
  /**
   * collection of input values.
   */
  protected Collection<T> collection;
  @Min(0)
  /**
   * size of output collection, all tuples till end window if set to 0.
   */
  private int size = 0;

  /**
   * Input port that takes data to be added to a collection.
   */
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

  /**
   * Output port that emits a collection.
   */
  public final transient DefaultOutputPort<Collection<T>> output = new DefaultOutputPort<Collection<T>>();

  /**
   * Set the size of the collection.
   *
   * If set to zero, the collection collects all the tuples within a window and
   * emits the collection as 1 output tuple at the end of the window. If set to
   * positive value, it collects the collection as soon as the size of the
   * collection reaches the size.
   *
   * @param size
   *          the size to set
   */
  public void setSize(int size)
  {
    this.size = size;
  }

  /**
   * Size of collection.
   *
   * @return size of collection
   */
  @Min(0)
  public int getSize()
  {
    return size;
  }

  /**
   * Abstract method to get collection of given size.
   *
   * @param size
   * @return collection
   */
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
}
