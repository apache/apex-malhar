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
package org.apache.apex.malhar.contrib.misc.streamquery.function;

import java.util.ArrayList;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;

/**
 * An implementation of function index that implements sql count function semantic. <br>
 * <p>
 * Counts number of values of given column and returns count of non null values in column.
 *   e.g : sql => SELECT COUNT(column_name) FROM table_name. <br>
 *   <br>
 *   <b> Properties : </b> <br>
 *   <b> column : </b> column name for values count.   <br>
 *   <b> alias  : </b> Alias name for aggregate output. <br>
 * @displayName Count Function
 * @category Stream Manipulators
 * @tags sql count
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class CountFunction extends FunctionIndex
{
  /**
   * @param column column for values count, must be non null.
   * @param alias  Alias name for aggregate output.
   */
  public CountFunction(@NotNull String column, String alias)
  {
    super(column, alias);
  }

  /**
   * Count number of values of given column.
   * @return Count of non null values in column.
   */
  @Override
  public Object compute(ArrayList<Map<String, Object>> rows) throws Exception
  {
    if (column.equals("*")) {
      return rows.size();
    }
    long count = 0;
    for (Map<String, Object> row : rows) {
      if (row.containsKey(column) && (row.get(column) != null)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Aggregate output name.
   * @return name string.
   */
  @Override
  protected String aggregateName()
  {
    if (!StringUtils.isEmpty(alias)) {
      return alias;
    }
    return "COUNT(" + column + ")";
  }

}
