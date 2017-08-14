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

/**
 * This is the base implementation for an aggregate output operator,
 * which writes to a transactionable store (over one application window).&nbsp;
 * This operator provides the exactly-once gaurantee.&nbsp;
 * Subclasses should implement that method which stores the aggregate state of the operator in the store.
 * <p></p>
 * @displayName Abstract Aggregate Transactionable Store Output
 * @category Output
 * @tags transactional, output operator
 *
 * @param <T> The tuple type.
 * @param <S> The store type.
 * @since 0.9.3
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
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
