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

import java.io.IOException;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 * This abstract class is intended to be an output adapter for a TransactionStore with "transactional exactly once" feature
 * For non-idempotent operations (e.g. incrementing values in the store, etc)
 *
 * @param <T> The type of the tuple
 * @param <S> The store type
 * @since 0.9.3
 */
public abstract class AbstractTransactionableStoreOutputOperator<T, S extends TransactionableStore> extends BaseOperator
{
  protected S store;
  protected String appId;
  protected Integer operatorId;
  protected long currentWindowId = -1;
  protected long committedWindowId = -1;
  /**
   * The input port
   */
  @InputPortFieldAnnotation(name = "in", optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      if (committedWindowId < currentWindowId) {
        processTuple(t);
      }
    }

  };

  /**
   * Gets the store
   *
   * @return the store
   */
  public S getStore()
  {
    return store;
  }

  /**
   * Sets the store
   *
   * @param store
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
      appId = context.getValue(DAG.APPLICATION_ID);
      operatorId = context.getId();
      committedWindowId = store.getCommittedWindowId(appId, operatorId);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }

  @Override
  public void teardown()
  {
    try {
      if (store.isInTransaction()) {
        store.rollbackTransaction();
      }
      store.disconnect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Processes the incoming tuple. Implementations need to provide what to do with the tuple (how to store the tuple)
   *
   * @param tuple
   */
  public abstract void processTuple(T tuple);

}
