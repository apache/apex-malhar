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

import java.util.List;
import java.util.Map;

/**
 * This interface is for any service that provides key/value storage
 *
 * @since 0.9.3
 */
public interface KeyValueStore extends Connectable
{
  /**
   * Gets the value given the key.
   *
   * @param key
   * @return the value
   */
  public Object get(Object key);

  /**
   * Gets all the values given by all the keys.
   * Optimally the implementation should take advantage of any bulk get call with the store
   *
   * @param keys
   * @return the list of values
   */
  public List<Object> getAll(List<Object> keys);

  /**
   * Sets the key with the value in the store.
   *
   * @param key
   * @param value
   */
  public void put(Object key, Object value);

  /**
   * Sets the keys with the values according to the given map
   *
   * @param m the map
   */
  public void putAll(Map<Object, Object> m);

  /**
   * Removes the key and the value given the key
   *
   * @param key
   */
  public void remove(Object key);
}
