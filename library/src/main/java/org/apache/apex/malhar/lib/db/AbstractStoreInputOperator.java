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
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * This is the base implementation of an input adapter which reads from a store.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p></p>
 * @displayName Abstract Store Input
 * @category Input
 *
 * @param <T> The tuple type
 * @param <S> The store type
 * @since 0.9.3
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractStoreInputOperator<T, S extends Connectable> implements InputOperator
{
  /**
   * The output port on which tuples read form a store are emitted.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();
  protected S store;
  /**
   * Gets the store.
   *
   * @return the store
   */
  public S getStore()
  {
    return store;
  }

  /**
   * Sets the store.
   *
   * @param store
   */
  public void setStore(S store)
  {
    this.store = store;
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
  public void setup(OperatorContext t1)
  {
    try {
      store.connect();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      store.disconnect();
    } catch (IOException ex) {
      // ignore
    }
  }
}
