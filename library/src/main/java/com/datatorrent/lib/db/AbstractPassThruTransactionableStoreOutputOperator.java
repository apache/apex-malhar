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
 * This abstract class is intended for pass-through output adapter of any transactional store that wants the "transactional exactly once" feature.
 * "Pass-through" means it does not wait for end window to write to the store. It will begin transaction at begin window and write to the store as the tuples
 * come and commit the transaction at end window.
 *
 * @param <T> The tuple type
 * @param <S> The store type
 * @since 0.9.3
 */
public abstract class AbstractPassThruTransactionableStoreOutputOperator<T, S extends TransactionableStore> extends AbstractTransactionableStoreOutputOperator<T, S>
{

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    store.beginTransaction();
  }

  @Override
  public void endWindow()
  {
    store.storeCommittedWindowId(appId, operatorId, currentWindowId);
    store.commitTransaction();
    committedWindowId = currentWindowId;
    super.endWindow();
  }

}
