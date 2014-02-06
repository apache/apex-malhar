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

/**
 * This abstract class is for aggregate output (over one application window) to a transactionable store with the transactional exactly-once feature.
 *
 * @param <T> The tuple type.
 * @param <S> The store type.
 * @since 0.9.3
 */
public abstract class AbstractAggregateTransactionableStoreOutputOperator<T, S extends TransactionableStore> extends AbstractTransactionableStoreOutputOperator<T, S>
{
  @Override
  public void endWindow()
  {
    store.beginTransaction();
    storeAggregate();
    store.storeCommittedWindowId(appId, operatorId, currentWindowId);
    store.commitTransaction();
    committedWindowId = currentWindowId;
    super.endWindow();
  }

  /**
   * Stores the aggregated state to persistent store at end window.
   *
   */
  public abstract void storeAggregate();
}
