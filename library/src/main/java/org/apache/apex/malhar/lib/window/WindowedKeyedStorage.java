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

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This interface is for a key/value store for storing data for windowed streams.
 * The key to this key/value store is a pair of (Window, K).
 * Also, this class may go away soon as there are plans to incorporate {@link Spillable} data structures
 * in the near future.
 *
 * Note that this interface expects that the implementation takes care of checkpoint recovery.
 *
 *
 * @since 3.5.0
 */
@InterfaceStability.Unstable
public interface WindowedKeyedStorage<K, V> extends WindowedStorage<Map<K, V>>
{
  /**
   * Sets the data associated with the given window and the key
   *
   * @param window
   * @param key
   * @param value
   */
  void put(Window window, K key, V value);

  /**
   * Gets the key/value pairs associated with the given window
   *
   * @param window
   * @return
   */
  Iterable<Map.Entry<K, V>> entrySet(Window window);

  /**
   * Gets the data associated with the given window and the key
   *
   * @param window
   * @param key
   * @return
   */
  V get(Window window, K key);

  /**
   * Removes all the data associated with the given window
   *
   * @param window
   */
  void remove(Window window);

  /**
   * Migrate the data from one window to another. This will invalidate fromWindow in the storage and move the
   * data to toWindow, and overwrite any existing data in toWindow
   *
   * @param fromWindow
   * @param toWindow
   */
  void migrateWindow(Window fromWindow, Window toWindow);
}
