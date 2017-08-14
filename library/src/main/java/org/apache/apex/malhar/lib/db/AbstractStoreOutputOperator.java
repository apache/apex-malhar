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
package org.apache.apex.malhar.lib.db;

import java.io.IOException;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is the base implementation of an output operator,
 * which writes to a non-transactional store.&nbsp;
 * This operator does not provide the exactly once guarantee.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p></p>
 * @displayName Abstract Store Output
 * @category Output
 *
 * @param <T> The tuple type
 * @param <S> The store type
 * @since 0.9.3
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractStoreOutputOperator<T, S extends Connectable> extends BaseOperator
{
  protected S store;

  /**
   * The input port on which tuples are received for writing.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }

  };

  /**
   * Gets the store.
   * @return the store.
   */
  public S getStore()
  {
    return store;
  }

  /**
   * Sets the store.
   * @param store a {@link Connectable}.
   */
  public void setStore(S store)
  {
    this.store = store;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      store.connect();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void teardown()
  {
    try {
      store.disconnect();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Processes the incoming tuple, presumably store the data in the tuple to the store
   *
   * @param tuple a tuple.
   */
  public abstract void processTuple(T tuple);

}
