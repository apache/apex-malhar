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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.SessionWindowedStorage;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;

/**
 * Spillable session windowed storage.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class SpillableSessionWindowedStorage<K, V> extends SpillableWindowedKeyedStorage<K, V> implements SessionWindowedStorage<K, V>
{
  // additional key to windows map for fast lookup of windows using key
  private Spillable.SpillableSetMultimap<K, Window.SessionWindow<K>> keyToWindowsMap;

  @Override
  @SuppressWarnings("unchecked")
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    if (keyToWindowsMap == null) {
      // NOTE: this will pose difficulties when we try to assign the entries to a time bucket later on.
      // This is logged in APEXMALHAR-2271
      // A work around to make session window data never expire and all kept in one time bucket
      keyToWindowsMap = scc.newSpillableSetMultimap(bucket, keySerde, (Serde<Window.SessionWindow<K>>)(Serde)windowSerde, new TimeExtractor.FixedTimeExtractor(Long.MAX_VALUE));
    }
  }



  @Override
  @SuppressWarnings("unchecked")
  public void remove(Window window)
  {
    super.remove(window);
    Window.SessionWindow<K> sessionWindow = (Window.SessionWindow<K>)window;
    keyToWindowsMap.remove(sessionWindow.getKey(), sessionWindow);
  }

  @Override
  public void put(Window window, K key, V value)
  {
    super.put(window, key, value);
    @SuppressWarnings("unchecked")
    Window.SessionWindow<K> sessionWindow = (Window.SessionWindow<K>)window;
    if (!keyToWindowsMap.containsEntry(key, sessionWindow)) {
      keyToWindowsMap.put(key, sessionWindow);
    }
  }

  @Override
  public void migrateWindow(Window.SessionWindow<K> fromWindow, Window.SessionWindow<K> toWindow)
  {
    K key = fromWindow.getKey();
    ImmutablePair<Window, K> oldKey = new ImmutablePair<Window, K>(fromWindow, key);
    ImmutablePair<Window, K> newKey = new ImmutablePair<Window, K>(toWindow, key);
    V value = windowKeyToValueMap.get(oldKey);
    if (value == null) {
      throw new NoSuchElementException();
    }
    windowKeyToValueMap.remove(oldKey);
    windowKeyToValueMap.put(newKey, value);
    keyToWindowsMap.remove(key, fromWindow);
    keyToWindowsMap.put(key, toWindow);
    windowToKeysMap.removeAll(fromWindow);
    windowToKeysMap.put(toWindow, key);
  }

  @Override
  public Collection<Map.Entry<Window.SessionWindow<K>, V>> getSessionEntries(K key, long timestamp, long gap)
  {
    List<Map.Entry<Window.SessionWindow<K>, V>> results = new ArrayList<>();
    Set<Window.SessionWindow<K>> sessionWindows = keyToWindowsMap.get(key);
    if (sessionWindows != null) {
      for (Window.SessionWindow<K> window : sessionWindows) {
        if (timestamp > window.getBeginTimestamp()) {
          if (window.getBeginTimestamp() + window.getDurationMillis() > timestamp) {
            results.add(new AbstractMap.SimpleEntry<>(window, windowKeyToValueMap.get(new ImmutablePair<Window, K>(window, key))));
          }
        } else if (timestamp < window.getBeginTimestamp()) {
          if (window.getBeginTimestamp() - gap <= timestamp) {
            results.add(new AbstractMap.SimpleEntry<>(window, windowKeyToValueMap.get(new ImmutablePair<Window, K>(window, key))));
          }
        } else {
          results.add(new AbstractMap.SimpleEntry<>(window, windowKeyToValueMap.get(new ImmutablePair<Window, K>(window, key))));
        }
      }
    }
    return results;
  }
}
