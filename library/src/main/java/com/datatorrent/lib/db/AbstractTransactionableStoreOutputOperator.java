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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import java.io.IOException;
import javax.validation.constraints.NotNull;

/**
 *
 * @since 0.9.3
 */
public abstract class AbstractTransactionableStoreOutputOperator<T, S extends TransactionableStore> extends BaseOperator
{
  protected S store;
  protected transient boolean inTransaction = false;
  protected String appId;
  protected Integer operatorId;
  protected long currentWindowId = -1;
  protected long committedWindowId = -1;

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

  public S getStore()
  {
    return store;
  }

  public void setStore(S store)
  {
    this.store = store;
  }

  public boolean isInTransaction()
  {
    return inTransaction;
  }

  public void setInTransaction(boolean inTransaction)
  {
    this.inTransaction = inTransaction;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      appId = context.getValue(DAG.APPLICATION_ID);
      operatorId = context.getId();

      store.connect();
      committedWindowId = getCommittedWindowId(appId, operatorId);
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

  /**
   * Gets the committed window id from a persistent store.
   *
   * @param appId
   * @param operatorId
   * @return the committed window id
   */
  protected abstract long getCommittedWindowId(String appId, int operatorId);

  /**
   * Stores the committed window id to a persistent store.
   *
   * @param appId
   * @param operatorId
   * @param windowId
   */
  protected abstract void storeCommittedWindowId(String appId, int operatorId, long windowId);

  @Override
  public void teardown()
  {
    try {
      if (inTransaction) {
        store.rollbackTransaction();
      }
      store.disconnect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Processes the incoming tuple.
   *
   * @param tuple
   */
  public abstract void processTuple(T tuple);

}
