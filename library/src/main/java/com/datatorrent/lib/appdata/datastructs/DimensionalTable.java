/*
 * Copyright (c) 2015 DataTorrent, Inc.
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a {@link DimensionalTable}. A {@link DimensionalTable} is similar to a Map but is a hybrid
 * between a conventional table and a map. Data in a {@link DimensionalTable} is organized into rows
 * and each row is broken into two parts, the key and the data payload. Each key is composed of a predefined list
 * of fields. A Diagram is below:
 * <br/>
 * <br/>
 * <table border="1">
 *  <tr>
 *    <td></td>
 *    <td>Key</td>
 *    <td>Value</td>
 *  </tr>
 *  <tr>
 *    <td></td>
 *    <td>
 *    <table border="1">
 *      <tr>
 *        <td>Header 1</td>
 *        <td>Header 2</td>
 *        <td>Header 3</td>
 *        <td>...</td>
 *      </tr>
 *    </table>
 *    </td>
 *    <td></td>
 *  </tr>
 * <tr>
 *  <td>
 *    Row 1
 *  </td>
 *  <td>
 *    <table border="1">
 *      <tr>
 *        <td>field 1&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
 *        <td>field 2&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
 *        <td>field 3&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
 *        <td>...</td>
 *      </tr>
 *    </table>
 *  </td>
 *  <td>Data payload</td>
 * </tr>
 * </table>
 * <br/>
 * <br/>
 * The purpose of this table is to provide a <b>select-like</b> feature for obtaining data payloads.
 * Selecting works as follows. If a user specifies one or more key components in a search query, but not
 * all the key components, then all the data payloads with a matching subset of key components are returned. If
 * all the key components are specified in a search query, then only a single data payload is returned if there
 * is a matching key, otherwise nothing is returned.
 *
 * @param <DATA> The type of the data payload.
 */
public class DimensionalTable<DATA>
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionalTable.class);

  /**
   * This is a map from the header name to its column index.
   */
  protected final Map<String, Integer> dimensionNameToIndex = Maps.newHashMap();

  /**
   * This is a column which holds the data payload.
   */
  protected final List<DATA> dataColumn = Lists.newArrayList();

  /**
   * These are the columns which hold each component of the key.
   */
  protected final List<List<Object>> dimensionColumns = Lists.newArrayList();

  /**
   * A map from a key row to its data payload.
   */
  protected final Map<List<Object>, DATA> dimensionKeysToData = Maps.newHashMap();

  /**
   * Constructor for Kryo
   */
  private DimensionalTable()
  {
    //For Kryo
  }

  /**
   * Creates a dimensional table with the given header names for key columns.
   * @param headerNames The header names for key columns.
   */
  public DimensionalTable(List<String> headerNames)
  {
    setHeaderNames(headerNames);

    initialize();
  }

  /**
   * Initializing the key element columns.
   */
  private void initialize()
  {
    for(int columnIndex = 0;
        columnIndex < dimensionNameToIndex.size();
        columnIndex++) {
      dimensionColumns.add(Lists.newArrayList());
    }
  }

  /**
   * Helper method to set and validate the header names for the table.
   * @param headerNames The head names for the key components of the table.
   */
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

  /**
   * Appends a row to the table. If the key combination for the row is not unique
   * then the existing row is replaced.
   * @param data The data payload for the row.
   * @param keys The values for the components of the keys. The key components
   * must be specified in the same order as their header names.
   */
  public void appendRow(DATA data, Object... keys)
  {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(keys);
    Preconditions.checkArgument(keys.length == dimensionNameToIndex.size(),
                                "All the dimension keys should be specified.");

    List<Object> keysList = Lists.newArrayList();

    for(Object key: keys) {
      keysList.add(key);
    }

    DATA prev = dimensionKeysToData.put(keysList, data);

    if(prev != null) {
      return;
    }

    dataColumn.add(data);

    for(int index = 0;
        index < keys.length;
        index++) {
      Object key = keys[index];
      dimensionColumns.get(index).add(key);
    }
  }

  /**
   * Appends a row to the table.
   * @param data The data payload for the row.
   * @param keys The values for the key components of the row. The key of the provided map corresponds to the
   * header name of a key component, and the value of the provided map corresponds to the value of a key component.
   */
  public void appendRow(DATA data, Map<String, ?> keys)
  {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(keys);

    Object[] keysArray = new Object[keys.size()];

    for(Map.Entry<String, ?> entry: keys.entrySet()) {
      String keyName = entry.getKey();
      Object value = entry.getValue();

      Preconditions.checkNotNull(keyName);

      Integer index = dimensionNameToIndex.get(keyName);
      keysArray[index] = value;
    }

    appendRow(data, keysArray);
  }

  /**
   * This method returns a data payload corresponding to the provided key, or null if there is no data payload
   * corresponding to the provided key.
   * @param keys A list containing the values of all the components of the key. The values of the key
   * components must be provided in the same order as their header names.
   * @return The data payload corresponding to the given key.
   */
  @SuppressWarnings("element-type-mismatch")
  public DATA getDataPoint(List<?> keys)
  {
    Preconditions.checkNotNull(keys);
    Preconditions.checkArgument(keys.size() == dimensionNameToIndex.size(),
                                "All the keys must be specified.");

    return dimensionKeysToData.get(keys);
  }

  /**
   * This method returns a data payload corresponding to the provided key, or null if there is no data payload
   * corresponding to the provided key.
   * @param keys The values for the key components of the row. The key of the provided map corresponds to the
   * header name of a key component, and the value of the provided map corresponds to the value of a key component.
   * @return The data payload corresponding to the given key.
   */
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

  /**
   * This method returns all the data payloads which correspond with the given subset of key components.
   * @param keys The values for the key components of the row. The key of the provided map corresponds to the
   * header name of a key component, and the value of the provided map corresponds to the value of a key component.
   * @return The data payloads corresponding to the given subset of key components.
   */
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
        Object keyColumn = keyColumns.get(columnIndex).get(rowIndex);

        if((key == null && keyColumn != null) ||
           (key != null && keyColumn == null) ||
           (key != null && keyColumn != null && !keyColumn.equals(key))) {
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

  /**
   * Returns all the data payloads in the table.
   * @return The data payload column of the table.
   */
  public List<DATA> getAllDataPoints()
  {
    return Lists.newArrayList(dataColumn);
  }

  /**
   * Returns the number of rows in the table.
   * @return The number of rows in the table.
   */
  public int size() {
    return dataColumn.size();
  }
}
