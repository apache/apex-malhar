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
 * An implementation of function index that implements sql first,last function semantic. <br>
 * <p>
 *   e.g : sql => SELECT FIRST/LAST(column_name) FROM table_name. <br>
 *   <br>
 *   <b> Properties : </b> <br>
 *   <b> column : </b> column name for first/last value.   <br>
 *   <b> alias  : </b> Alias name for output. <br>
 *   <b> isFirst : </b> return first value if true.
 * @displayName First Last Function
 * @category Stream Manipulators
 * @tags sql first, sql last
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class FirstLastFunction extends FunctionIndex
{
  /**
   * return first value if true.
   */
  private boolean isFirst;

  /**
   * @param column  column name for first/last value.
   * @param  alias   Alias name for output.
   * @param  isFirst return first value if true.
   */
  public FirstLastFunction(@NotNull String column, String alias, boolean isLast)
  {
    super(column, alias);
    isFirst = !isLast;
  }

  /**
   * Get first/last non null value for column.
   */
  @Override
  public Object compute(@NotNull ArrayList<Map<String, Object>> rows) throws Exception
  {
    if (rows.size() == 0) {
      return null;
    }
    if (isFirst) {
      for (int i = 0; i < rows.size(); i++) {
        if (rows.get(i).get(column) != null) {
          return rows.get(i).get(column);
        }
      }
    } else {
      for (int i = (rows.size() - 1); i >= 0; i--) {
        if (rows.get(i).get(column) != null) {
          return rows.get(i).get(column);
        }
      }
    }
    return null;
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
    if (isFirst) {
      return "FIRST(" + column + ")";
    }
    return "LAST(" + column + ")";
  }

  public boolean isFirst()
  {
    return isFirst;
  }

  public void setFirst(boolean isFirst)
  {
    this.isFirst = isFirst;
  }

}
