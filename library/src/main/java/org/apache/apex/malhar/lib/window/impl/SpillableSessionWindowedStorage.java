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

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.window.SessionWindowedStorage;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Spillable session windowed storage. Note that the underlying implementation does not support certain operations so
 * this class is not usable yet
 */
@InterfaceStability.Evolving
public class SpillableSessionWindowedStorage<K, V> extends SpillableWindowedKeyedStorage<K, V> implements SessionWindowedStorage<K, V>
{
  // additional key to windows map for fast lookup of windows using key
  protected Spillable.SpillableByteArrayListMultimap<K, Window.SessionWindow<K>> keyToWindowsMap;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    if (keyToWindowsMap == null) {
      keyToWindowsMap = scc.newSpillableByteArrayListMultimap(bucket, keySerde, (Serde<Window.SessionWindow<K>, Slice>)(Serde)windowSerde);
    }
  }

  @Override
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
    if (!keyToWindowsMap.containsEntry(key, (Window.SessionWindow<K>)window)) {
      keyToWindowsMap.put(key, (Window.SessionWindow<K>)window);
    }
  }

  @Override
  public void migrateWindow(Window fromWindow, Window toWindow)
  {
    List<K> keys = windowToKeysMap.get(fromWindow);
    windowKeyToValueMap.remove(toWindow);
    for (K key : keys) {
      windowToKeysMap.put(toWindow, key);
      ImmutablePair<Window, K> oldKey = new ImmutablePair<>(fromWindow, key);
      ImmutablePair<Window, K> newKey = new ImmutablePair<>(toWindow, key);

      V value = windowKeyToValueMap.get(oldKey);
      windowKeyToValueMap.remove(oldKey);
      windowKeyToValueMap.put(newKey, value);
      keyToWindowsMap.remove(key, (Window.SessionWindow<K>)fromWindow);
      keyToWindowsMap.put(key, (Window.SessionWindow<K>)toWindow);
    }
    windowToKeysMap.removeAll(fromWindow);
  }

  @Override
  public Collection<Map.Entry<Window.SessionWindow<K>, V>> getSessionEntries(K key, long timestamp, long gap)
  {
    List<Map.Entry<Window.SessionWindow<K>, V>> results = new ArrayList<>();
    List<Window.SessionWindow<K>> sessionWindows = keyToWindowsMap.get(key);
    if (sessionWindows != null) {
      for (Window.SessionWindow<K> window : sessionWindows) {
        if (timestamp > window.getBeginTimestamp()) {
          if (window.getBeginTimestamp() + window.getDurationMillis() + gap > timestamp) {
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
