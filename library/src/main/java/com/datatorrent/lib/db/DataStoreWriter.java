/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

import java.util.List;

import com.datatorrent.lib.db.Connectable;

/**
 * Interface for doing batch inserts to the data store
 * @param <T> tuple type
 */
public interface DataStoreWriter<T> extends Connectable
{
  /**
   * For each window id, performs batch insert of the list of tuples
   * @param tupleList list of tuples
   * @param windowId windowId for the batch
   */
  public void batchInsert(List<T> tupleList, long windowId);

  /**
   * For each windowId, performs batch insert and updates existing tuples on the upsert key
   * @param tupleList list of tuples
   * @param upsertKey upsert key for updates
   * @param windowId windowId of the batch
   */
  public void batchUpsert(List<T> tupleList, List<String> upsertKey, long windowId);

}
