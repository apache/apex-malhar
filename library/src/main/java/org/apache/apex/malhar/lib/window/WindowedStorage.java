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

/**
 * WindowedStorage is a key-value store with the key being the window. The implementation of this interface should
 * make sure checkpointing and recovery will be done correctly.
 *
 * @param <T> The type of the data that is stored per window
 *
 * TODO: Look at the possibility of integrating spillable data structure: https://issues.apache.org/jira/browse/APEXMALHAR-2026
 */
@InterfaceStability.Evolving
public interface WindowedStorage<T> extends Iterable<Map.Entry<Window, T>>
{
  /**
   * Returns true if the storage contains this window
   *
   * @param window
   */
  boolean containsWindow(Window window);

  /**
   * Returns the number of windows in the storage
   *
   * @return
   */
  long size();

  /**
   * Sets the data associated with the given window
   *
   * @param window
   * @param value
   */
  void put(Window window, T value);

  /**
   * Gets the value associated with the given window
   *
   * @param window
   * @return
   */
  T get(Window window);

  /**
   * Removes all the data associated with the given window. This does NOT mean removing the window in checkpointed state
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

  /**
   * Returns the iterable of the entries in the storage
   *
   * @return
   */
  Iterable<Map.Entry<Window, T>> entrySet();
}
