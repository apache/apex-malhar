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
package org.apache.apex.malhar.lib.window.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.apex.malhar.lib.window.SessionWindowedStorage;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is the in-memory implementation of {@link WindowedKeyedStorage}. Do not use this class if you have a large state that
 * can't be fit in memory.
 *
 * @since 3.5.0
 */
@InterfaceStability.Unstable
public class InMemorySessionWindowedStorage<K, V> extends InMemoryWindowedKeyedStorage<K, V>
    implements SessionWindowedStorage<K, V>
{
  private Map<K, TreeSet<Window.SessionWindow<K>>> keyToWindows = new HashMap<>();

  @Override
  public void put(Window window, K key, V value)
  {
    super.put(window, key, value);
    TreeSet<Window.SessionWindow<K>> sessionWindows = keyToWindows.get(key);
    if (sessionWindows == null) {
      sessionWindows = new TreeSet<>();
      keyToWindows.put(key, sessionWindows);
    }
    sessionWindows.add((Window.SessionWindow<K>)window);
  }

  @Override
  public void migrateWindow(Window.SessionWindow<K> fromWindow, Window.SessionWindow<K> toWindow)
  {
    if (containsWindow(fromWindow)) {
      map.put(toWindow, map.remove(fromWindow));
    }
  }

  @Override
  public Collection<Map.Entry<Window.SessionWindow<K>, V>> getSessionEntries(K key, long timestamp, long gap)
  {
    List<Map.Entry<Window.SessionWindow<K>, V>> results = new ArrayList<>();
    TreeSet<Window.SessionWindow<K>> sessionWindows = keyToWindows.get(key);
    if (sessionWindows != null) {
      Window.SessionWindow<K> refWindow = new Window.SessionWindow<>(key, timestamp, 1);
      Window.SessionWindow<K> floor = sessionWindows.floor(refWindow);
      if (floor != null) {
        if (floor.getBeginTimestamp() + floor.getDurationMillis() + gap > timestamp) {
          results.add(new AbstractMap.SimpleEntry<>(floor, map.get(floor).get(key)));
        }
      }
      Window.SessionWindow<K> higher = sessionWindows.higher(refWindow);
      if (higher != null) {
        if (higher.getBeginTimestamp() - gap <= timestamp) {
          results.add(new AbstractMap.SimpleEntry<>(higher, map.get(higher).get(key)));
        }
      }
    }
    return results;
  }
}
