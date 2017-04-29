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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is the in-memory implementation of {@link WindowedKeyedStorage}. Do not use this class if you have a large state that
 * can't be fit in memory.
 *
 * @since 3.5.0
 */
@InterfaceStability.Unstable
public class InMemoryWindowedKeyedStorage<K, V> extends InMemoryWindowedStorage<Map<K, V>>
    implements WindowedStorage.WindowedKeyedStorage<K, V>
{
  @Override
  public void put(Window window, K key, V value)
  {
    Map<K, V> kvMap;
    if (map.containsKey(window)) {
      kvMap = map.get(window);
    } else {
      kvMap = new HashMap<>();
      map.put(window, kvMap);
    }
    kvMap.put(key, value);
  }

  public Iterable<Map.Entry<K, V>> entries(Window window)
  {
    if (map.containsKey(window)) {
      return map.get(window).entrySet();
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public V get(Window window, K key)
  {
    if (map.containsKey(window)) {
      return map.get(window).get(key);
    } else {
      return null;
    }
  }

}
