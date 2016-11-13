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
 * An implementation of function index that implements average function semantics. <br>
 * <p>
 *   e.g : sql => SELECT AVG(column_name) FROM table_name. <br>
 *   <br>
 *   <b> Properties : </b> <br>
 *   <b> column : </b> Aggregate over given column values.   <br>
 *   <b> alias  : </b> Alias name for aggregate output. <br>
 * @displayName Average Function
 * @category Stream Manipulators
 * @tags sql average
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class AverageFunction extends FunctionIndex
{
  /**
   * @param column Aggregate over given column values, must be non null.
   * @param alias  Alias name for aggregate output.
   */
  public AverageFunction(@NotNull String column, String alias)
  {
    super(column, alias);
  }

  /**
   * Compute average for given column values.
   */
  @Override
  public Object compute(@NotNull ArrayList<Map<String, Object>> rows) throws Exception
  {
    if (rows.size() == 0) {
      return 0.0;
    }
    double sum = 0.0;
    for (Map<String, Object> row : rows) {
      sum += ((Number)row.get(column)).doubleValue();
    }
    return sum / rows.size();
  }

  /**
   * Get aggregate name.
   * @return name.
   */
  @Override
  protected String aggregateName()
  {
    if (!StringUtils.isEmpty(alias)) {
      return alias;
    }
    return "AVG(" + column + ")";
  }
}
