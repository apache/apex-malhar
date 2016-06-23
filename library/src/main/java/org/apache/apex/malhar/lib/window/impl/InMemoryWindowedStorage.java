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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is the in-memory implementation of WindowedStorage. Do not use this class if you have a large state that
 * can't be fit in memory.
 */
@InterfaceStability.Evolving
public class InMemoryWindowedStorage<T> implements WindowedStorage<T>
{
  protected final TreeMap<Window, T> map = new TreeMap<>(Window.DEFAULT_COMPARATOR);

  @Override
  public long size()
  {
    return map.size();
  }

  @Override
  public void put(Window window, T value)
  {
    map.put(window, value);
  }

  @Override
  public boolean containsWindow(Window window)
  {
    return map.containsKey(window);
  }

  @Override
  public T get(Window window)
  {
    return map.get(window);
  }

  @Override
  public void remove(Window window)
  {
    map.remove(window);
  }

  @Override
  public void migrateWindow(Window fromWindow, Window toWindow)
  {
    if (containsWindow(fromWindow)) {
      map.put(toWindow, map.remove(fromWindow));
    }
  }

  @Override
  public Iterable<Map.Entry<Window, T>> entrySet()
  {
    return map.entrySet();
  }

  @Override
  public Iterator<Map.Entry<Window, T>> iterator()
  {
    return map.entrySet().iterator();
  }
}
