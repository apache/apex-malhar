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
 * This is the base implementation for a pass-through output adapter of a transactional key value store.&nbsp;
 * This implementation provides the "exactly once" guarantee.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p>
 * "Pass-through" means it does not wait for end window to write to the store. It will begin transaction at begin window and write to the store as the tuples
 * come and commit the transaction at end window.
 * </p>
 * @displayName Abstract Pass Through Transactionable Key Value Store Output
 * @category Outupt
 * @tags key value, transactional
 *
 * @param <T> The tuple type
 * @param <S> The store type
 * @since 0.9.3
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractPassThruTransactionableKeyValueStoreOutputOperator<T, S extends TransactionableKeyValueStore>
    extends AbstractPassThruTransactionableStoreOutputOperator<T, S>
{
}
