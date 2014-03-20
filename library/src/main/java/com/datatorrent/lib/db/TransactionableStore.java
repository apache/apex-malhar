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
 * This interface is for a store that supports transactions
 *
 * @since 0.9.3
 */
public interface TransactionableStore extends Transactionable, Connectable
{
  /**
   * Gets the committed window id from a persistent store.
   *
   * @param appId      application id
   * @param operatorId operator id
   * @return the committed window id
   */
  public long getCommittedWindowId(String appId, int operatorId);

  /**
   * Stores the committed window id to a persistent store.
   *
   * @param appId      application id
   * @param operatorId operator id
   * @param windowId   window id
   */
  public void storeCommittedWindowId(String appId, int operatorId, long windowId);

  /**
   * Removes the committed window id from a persistent store.
   *
   * @param appId
   * @param operatorId
   */
  public void removeCommittedWindowId(String appId, int operatorId);
}
