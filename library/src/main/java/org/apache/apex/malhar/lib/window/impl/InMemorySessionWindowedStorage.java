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
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.apex.malhar.lib.window.SessionWindowedStorage;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Supplier;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SortedSetMultimap;

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
  private SortedSetMultimap<K, Window.SessionWindow<K>> keyToWindows = Multimaps.newSortedSetMultimap(new HashMap<K, Collection<Window.SessionWindow<K>>>(), new Supplier<SortedSet<Window.SessionWindow<K>>>()
  {
    @Override
    public SortedSet<Window.SessionWindow<K>> get()
    {
      return new TreeSet<>();
    }
  });

  @Override
  public void put(Window window, K key, V value)
  {
    @SuppressWarnings("unchecked")
    Window.SessionWindow<K> sessionWindow = (Window.SessionWindow<K>)window;
    super.put(window, key, value);
    keyToWindows.put(key, sessionWindow);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void remove(Window window)
  {
    super.remove(window);
    Window.SessionWindow<K> sessionWindow = (Window.SessionWindow<K>)window;
    keyToWindows.remove(sessionWindow.getKey(), sessionWindow);
  }

  @Override
  public void migrateWindow(Window.SessionWindow<K> fromWindow, Window.SessionWindow<K> toWindow)
  {
    if (!containsWindow(fromWindow)) {
      throw new NoSuchElementException();
    }
    map.put(toWindow, map.remove(fromWindow));
    keyToWindows.remove(fromWindow.getKey(), fromWindow);
    keyToWindows.put(toWindow.getKey(), toWindow);
  }

  @Override
  public Collection<Map.Entry<Window.SessionWindow<K>, V>> getSessionEntries(K key, long timestamp, long gap)
  {
    List<Map.Entry<Window.SessionWindow<K>, V>> results = new ArrayList<>();
    SortedSet<Window.SessionWindow<K>> sessionWindows = keyToWindows.get(key);
    if (sessionWindows != null) {
      Window.SessionWindow<K> refWindow = new Window.SessionWindow<>(key, timestamp, gap);
      SortedSet<Window.SessionWindow<K>> headSet = sessionWindows.headSet(refWindow);
      if (!headSet.isEmpty()) {
        Window.SessionWindow<K> lower = headSet.last();
        if (lower.getBeginTimestamp() + lower.getDurationMillis() > timestamp) {
          results.add(new AbstractMap.SimpleEntry<>(lower, map.get(lower).get(key)));
        }
      }
      SortedSet<Window.SessionWindow<K>> tailSet = sessionWindows.tailSet(refWindow);
      if (!tailSet.isEmpty()) {
        Window.SessionWindow<K> higher = tailSet.first();
        if (higher.getBeginTimestamp() - gap <= timestamp) {
          results.add(new AbstractMap.SimpleEntry<>(higher, map.get(higher).get(key)));
        }
      }
    }
    return results;
  }
}
