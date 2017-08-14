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
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is the base implementation of an output operator,
 * which writes to a transactional store.&nbsp;
 * This operator does not provide the exactly once guarantee.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 *
 * with "transactional exactly once" feature
 * For non-idempotent operations (incrementing values in the store, etc).
 * <p></p>
 * @displayName Abstract Transactionable Store Output
 * @category Output
 * @tags transactional
 *
 * @param <T> The type of the tuple
 * @param <S> The store type
 * @since 0.9.3
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractTransactionableStoreOutputOperator<T, S extends TransactionableStore> extends BaseOperator
{
  protected S store;
  protected String appId;
  protected Integer operatorId;
  protected long currentWindowId = -1;
  protected long committedWindowId = -1;
  /**
   * The input port on which tuples are received for writing.
   */
  @InputPortFieldAnnotation(optional = true)
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
    } catch (IOException ex) {
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
    } catch (IOException ex) {
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
