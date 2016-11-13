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
package org.apache.apex.malhar.contrib.misc.streamquery;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Implements order by key name rule. <br>
 * <p>
 * <b>Properties : </b> <br>
 * <b> columnName : </b> Name of column for ordering tuples. <br>
 * @displayName OrderBy Rule
 * @category Stream Manipulators
 * @tags orderby, sort, comparison
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
@SuppressWarnings("rawtypes")
public class OrderByRule<T extends Comparable>
{

  /**
   * column name for ordering tuples.
   */
  private String columnName;

  public OrderByRule(String name)
  {

    columnName = name;
  }

  /**
   * sort rows by each rule and emit result on output port.
   */
  @SuppressWarnings("unchecked")
  public ArrayList<Map<String, Object>> sort(ArrayList<Map<String, Object>> rows)
  {

    TreeMap<T, ArrayList<Map<String, Object>>> sorted = new TreeMap<T, ArrayList<Map<String, Object>>>();
    for (int i = 0; i < rows.size(); i++) {
      Map<String, Object> row = rows.get(i);
      if (row.containsKey(columnName)) {
        T value = (T)row.get(columnName);
        ArrayList<Map<String, Object>> list;
        if (sorted.containsKey(value)) {
          list = sorted.get(value);
        } else {
          list = new ArrayList<Map<String, Object>>();
          sorted.put(value, list);
        }
        list.add(row);
      }
    }
    ArrayList<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    for (Map.Entry<T, ArrayList<Map<String, Object>>> entry : sorted.entrySet()) {
      result.addAll(entry.getValue());
    }
    return result;
  }

  /**
   * @return the columnName
   */
  public String getColumnName()
  {

    return columnName;
  }

  /**
   * @param columnName
   *          the columnName to set
   */
  public void setColumnName(String columnName)
  {

    this.columnName = columnName;
  }
}
