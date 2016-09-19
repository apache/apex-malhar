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
 * An implementation of function index that implements sql max and sql min function semantic. <br>
 * <p>
 *   e.g : sql => SELECT MAX/MIN(column_name) FROM table_name. <br>
 *   <br>
 *   <b> Properties : </b> <br>
 *   <b> column : </b> column name for values max/min computation.   <br>
 *   <b> alias  : </b> Alias name for  output value. <br>
 *   <b> isMax : </b> Flag to indicate max/min compute value. <br>
 * @displayName Max Min Function
 * @category Stream Manipulators
 * @tags sql max, sql min
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class MaxMinFunction extends FunctionIndex
{
  /**
   * Flag to indicate max/min compute value, compute max if true.
   */
  private boolean isMax = true;

  /**
   * @param column column name for values max/min computation.   <br>
   * @param alias  Alias name for output. <br>
   * @param isMax  Flag to indicate max/min compute value. <br>
   */
  public MaxMinFunction(@NotNull String column, String alias, boolean isMin)
  {
    super(column, alias);
    isMax = !isMin;
  }

  /**
   * Compute max/min for given column.
   * @return max/min value.
   */
  @Override
  public Object compute(ArrayList<Map<String, Object>> rows) throws Exception
  {
    double minMax = 0.0;
    for (Map<String, Object> row : rows) {
      double value = ((Number)row.get(column)).doubleValue();
      if ((isMax && (minMax < value)) || (!isMax && (minMax > value))) {
        minMax = value;
      }
    }
    return minMax;
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
    if (isMax) {
      return "MAX(" + column + ")";
    }
    return "MIN(" + column + ")";
  }

  public boolean isMax()
  {
    return isMax;
  }

  public void setMax(boolean isMax)
  {
    this.isMax = isMax;
  }

}
