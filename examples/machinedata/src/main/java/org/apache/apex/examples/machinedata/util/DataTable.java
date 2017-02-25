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
package org.apache.apex.examples.machinedata.util;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;

/**
 * <p>DataTable class.</p>
 *
 * @since 0.3.5
 */
public class DataTable<R,C,E>
{

  //machineKey, [cpu,ram,hdd] -> value
  private final Map<R,Map<C,E>> table = Maps.newHashMap();

  public boolean containsRow(R rowKey)
  {
    return table.containsKey(rowKey);
  }

  public void put(R rowKey,C colKey, E entry)
  {
    if (!containsRow(rowKey)) {
      table.put(rowKey, Maps.<C,E>newHashMap());
    }
    table.get(rowKey).put(colKey, entry);
  }

  @Nullable
  public E get(R rowKey, C colKey)
  {
    if (!containsRow(rowKey)) {
      return null;
    }
    return table.get(rowKey).get(colKey);
  }

  public Set<R> rowKeySet()
  {
    return table.keySet();
  }

  public void clear()
  {
    table.clear();
  }

  public Map<R,Map<C,E>> getTable()
  {
    return table;
  }
}
