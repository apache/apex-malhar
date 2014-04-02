/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.database;

import java.util.Map;
import java.util.Set;

/**
 * <br>A store which could be a memory store or a database store.</br>
 *
 * @since 0.9.2
 */
public interface Store
{
  /**
   * <br>Retrieve the value of a key.</br>
   *
   * @param key the key to look for.
   * @return value of the key.
   */
  Object getValueFor(Object key);

  /**
   * <br>Retrieve the values of a set of keys.</br>
   *
   * @param keys keys to look for.
   * @return mapping of keys to their values.
   */
  Map<Object, Object> bulkGet(Set<Object> keys);

  /**
   * Perform shutdown.
   */
  void shutdownStore();

  /**
   * <br>A primary store should also provide setting the value for a key.</br>
   */
  public static interface Primary extends Store
  {
    /**
     * Sets the value of a key in the store.
     *
     * @param key   key
     * @param value value of the key.
     */
    void setValueFor(Object key, Object value);

    /**
     * Get all the keys in the store.
     *
     * @return all present keys.
     */
    Set<Object> getKeys();

    /**
     * Bulk set metric.
     *
     * @param data mapping of keys to values which will be saved.
     */
    void bulkSet(Map<Object, Object> data);
  }

  /**
   * <br>Backup store is queried when {@link Primary} doesn't contain a key.</br>
   * <br>It also provides data needed at startup.</br>
   */
  public static interface Backup extends Store
  {
    /**
     * <br>Backup stores are also used to initialize primary stores. This fetches initialization data.</br>
     *
     * @return map of key/value to initialize {@link StoreManager}
     */
    Map<Object, Object> fetchStartupData();
  }

}
