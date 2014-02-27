/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.db;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import java.io.IOException;

/**
 * This abstract class is for any implementation of an input adapter of a store {@link Connectable}
 *
 * @param <T> The tuple type
 * @param <S> The store type
 * @since 0.9.3
 */
public abstract class AbstractStoreInputOperator<T, S extends Connectable> implements InputOperator
{
  /**
   * The output port.
   */
  @OutputPortFieldAnnotation(name = "out")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();
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
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      store.disconnect();
    }
    catch (IOException ex) {
      // ignore
    }
  }
}
