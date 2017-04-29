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
package org.apache.apex.malhar.lib.window;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;

/**
 * WindowedStorage is a key-value store with the key being the window. The implementation of this interface should
 * make sure checkpointing and recovery will be done correctly.
 *
 * @since 3.5.0
 */
@InterfaceStability.Unstable
public interface WindowedStorage extends Component<Context.OperatorContext>
{
  /**
   * Returns true if the storage contains this window
   *
   * @param window the window
   */
  boolean containsWindow(Window window);

  /**
   * Returns the number of windows in the storage
   *
   * @return the number of windows
   */
  long size();

  /**
   * Removes all the data associated with the given window. This does NOT mean removing the window in checkpointed state
   *
   * @param window the window
   */
  void remove(Window window);

  /**
   * Purge checkpointed data for all the windows that lie totally beyond the given horizon
   */
  void purge(long horizonMillis);

  /**
   * This interface handles plain value per window. If there is a key/value map for each window, use
   * {@link WindowedKeyedStorage}. Also note that a single T object is assumed to be fit in memory
   *
   * Note that this interface expects that the implementation takes care of checkpoint recovery.
   *
   * @param <T> The type of the data that is stored per window
   */
  interface WindowedPlainStorage<T> extends WindowedStorage
  {
    /**
     * Sets the data associated with the given window
     *
     * @param window the window
     * @param value the value
     */
    void put(Window window, T value);

    /**
     * Gets the value associated with the given window
     *
     * @param window the window
     * @return the value
     */
    T get(Window window);

    /**
     * Returns the iterable of the entries in the storage
     *
     * @return the entries
     */
    Iterable<Map.Entry<Window, T>> entries();
  }

  /**
   * This interface is for a store that maps Windows to maps of key value pairs.
   *
   * Note that this interface expects that the implementation takes care of checkpoint recovery.
   *
   */
  interface WindowedKeyedStorage<K, V> extends WindowedStorage
  {
    /**
     * Sets the data associated with the given window and the key
     *
     * @param window the window
     * @param key the key
     * @param value the value
     */
    void put(Window window, K key, V value);

    /**
     * Gets an iterable object over the key/value pairs associated with the given window
     *
     * @param window the window
     * @return the entries
     */
    Iterable<Map.Entry<K, V>> entries(Window window);

    /**
     * Gets the data associated with the given window and the key
     *
     * @param window the window
     * @param key the key
     * @return the value
     */
    V get(Window window, K key);

  }
}
