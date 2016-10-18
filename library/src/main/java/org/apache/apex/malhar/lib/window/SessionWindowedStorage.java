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

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * This interface is for storing data for session windowed streams.
 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface SessionWindowedStorage<K, V> extends WindowedStorage.WindowedKeyedStorage<K, V>
{
  /**
   * Migrate the data from one window to another. This will invalidate fromWindow in the storage and move the
   * data to toWindow, and overwrite any existing data in toWindow
   *
   * @param fromWindow the window we want to migrate from
   * @param toWindow the window we want to migrate to
   */
  void migrateWindow(Window.SessionWindow<K> fromWindow, Window.SessionWindow<K> toWindow);

  /**
   * Given the key, the timestamp and the gap, gets the windows that overlaps with timestamp to (timestamp + gap).
   * This is used for getting the windows the timestamp belongs to, and for determining whether to merge
   * session windows.
   * This should only return at most two windows if sessions have been merged appropriately.
   *
   * @param key the key
   * @param timestamp the timestamp
   * @param gap the minimum gap
   * @return the windows
   */
  Collection<Map.Entry<Window.SessionWindow<K>, V>> getSessionEntries(K key, long timestamp, long gap);
}
