/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.appdata.datastructs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DimensionalTable<DATA>
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionalTable.class);

  private final Map<String, Integer> dimensionNameToIndex = Maps.newHashMap();

  private final List<DATA> dataColumn = Lists.newArrayList();
  private final List<List<Object>> dimensionColumns = Lists.newArrayList();

  private final Map<List<Object>, DATA> dimensionKeysToData = Maps.newHashMap();

  private DimensionalTable()
  {
    //For Kryo
  }

  public DimensionalTable(List<String> headerNames)
  {
    setHeaderNames(headerNames);

    initialize();
  }

  private void initialize()
  {
    for(int columnIndex = 0;
        columnIndex < dimensionNameToIndex.size();
        columnIndex++) {
      dimensionColumns.add(Lists.newArrayList());
    }
  }

  private void setHeaderNames(List<String> headerNames)
  {
    Preconditions.checkNotNull(headerNames);
    Preconditions.checkArgument(!headerNames.isEmpty(), "headerNames");

    for(String headerName: headerNames) {
      Preconditions.checkNotNull(headerName);
    }

    Set<String> headerNameSet = Sets.newHashSet(headerNames);

    Preconditions.checkArgument(headerNameSet.size() == headerNames.size(),
                                "The provided list of header names has duplicate names: " +
                                headerNames);

    for(int index = 0;
        index < headerNames.size();
        index++) {
      dimensionNameToIndex.put(headerNames.get(index), index);
    }
  }

  public void appendRow(DATA data, Object... keys)
  {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(keys);
    Preconditions.checkArgument(keys.length == dimensionNameToIndex.size(),
                                "All the dimension keys should be specified.");

    dataColumn.add(data);

    List<Object> keysList = Lists.newArrayList();

    for(Object key: keys) {
      Preconditions.checkNotNull(key);
      keysList.add(key);
    }

    DATA prev = dimensionKeysToData.put(keysList, data);
    logger.debug("prev {}", prev);

    Preconditions.checkState(prev == null,
                             "The given keys must be unique " + Arrays.toString(keys));

    for(int index = 0;
        index < keys.length;
        index++) {
      Object key = keys[index];
      dimensionColumns.get(index).add(key);
    }
  }

  public void appendRow(DATA data, Map<String, ?> keys)
  {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(keys);

    Object[] keysArray = new Object[keys.size()];

    for(Map.Entry<String, ?> entry: keys.entrySet()) {
      String keyName = entry.getKey();
      Object value = entry.getValue();

      Preconditions.checkNotNull(keyName);
      Preconditions.checkNotNull(value);

      Integer index = dimensionNameToIndex.get(keyName);
      keysArray[index] = value;
    }

    appendRow(data, keysArray);
  }

  public DATA getDataPoint(List<?> keys)
  {
    Preconditions.checkNotNull(keys);
    Preconditions.checkArgument(keys.size() == dimensionNameToIndex.size(),
                                "All the keys must be specified.");

    return dimensionKeysToData.get(keys);
  }

  public DATA getDataPoint(Map<String, ?> keys)
  {
    Preconditions.checkNotNull(keys);
    Preconditions.checkArgument(keys.size() == dimensionNameToIndex.size(),
                                "All the keys must be specified.");

    List<Object> keysList = Lists.newArrayList();

    for(int index = 0;
        index < dimensionNameToIndex.size();
        index++) {
      keysList.add(null);
    }

    for(Map.Entry<String, ?> entry: keys.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      Integer index = dimensionNameToIndex.get(key);

      keysList.set(index, value);
    }

    return getDataPoint(keysList);
  }

  public List<DATA> getDataPoints(Map<String, ?> keys)
  {
    Preconditions.checkNotNull(keys);

    Preconditions.checkArgument(dimensionNameToIndex.keySet().containsAll(keys.keySet()),
                                "The given keys contain names which are not valid keys.");

    List<Integer> indices = Lists.newArrayList();
    List<List<Object>> keyColumns = Lists.newArrayList();

    Map<Integer, Object> indexToKey = Maps.newHashMap();

    for(Map.Entry<String, ?> entry: keys.entrySet()) {
      String dimensionName = entry.getKey();
      Object value = entry.getValue();
      Integer index = dimensionNameToIndex.get(dimensionName);

      indices.add(index);
      indexToKey.put(index, value);
    }

    Collections.sort(indices);
    List<Object> tempKeys = Lists.newArrayList();

    for(Integer index: indices) {
      tempKeys.add(indexToKey.get(index));
      keyColumns.add(dimensionColumns.get(index));
    }

    int numRows = keyColumns.get(0).size();
    List<DATA> results = Lists.newArrayList();

    for(int rowIndex = 0;
        rowIndex < numRows;
        rowIndex++)
    {
      boolean allEqual = true;

      for(int columnIndex = 0;
          columnIndex < tempKeys.size();
          columnIndex++) {
        Object key = tempKeys.get(columnIndex);

        if(!keyColumns.get(columnIndex).get(rowIndex).equals(key)) {
          allEqual = false;
          break;
        }
      }

      if(allEqual) {
        results.add(dataColumn.get(rowIndex));
      }
    }

    return results;
  }
}
