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

import java.util.Collection;
import java.util.List;

/**
 * Interface for writing tuples to the data store
 * @param <T>
 */
public interface DataStoreWriter<T> extends Connectable
{
  /**
   * Write the tuple to the data store
   * @param tuple
   */
  public void process(T tuple);

  /**
   * Bulk write/update tuples to the data store once per window
   * @param tuples tuples in the window
   * @param windowId windowId of the window
   */
  public void processBulk(Collection<T> tuples, long windowId);

  /**
   * retreive if tuple exists in the data store
   * @param tuple tuple to look for
   * @return tuple
   */
  public T retreive(T tuple);

  /**
   * returns the last updated windowId
   * @return
   */
  public long retreiveLastUpdatedWindowId();
}
