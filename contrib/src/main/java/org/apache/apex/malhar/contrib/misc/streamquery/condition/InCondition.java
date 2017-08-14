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
package org.apache.apex.malhar.contrib.misc.streamquery.condition;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.streamquery.condition.Condition;

/**
 * An implementation of condition class to check if a column value is in a given set of values.
 * <p>
 * <br>
 * <b>Properties : </b> <br>
 * <b> column : </b> Column name for which value is checked in values set. <br>
 * <b> inValues : </b> Set of values in which column value is checked. <br>
 * @displayName In Condition
 * @category Stream Manipulators
 * @tags sql condition
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class InCondition extends Condition
{
  /**
   * Column name for which value is checked in values set.
   */
  @NotNull
  private String column;

  /**
   * Set of values in which column value is checked.
   */
  private Set<Object> inValues = new HashSet<Object>();

  /**
   * @param column Column name for which value is checked in values set.
   */
  public InCondition(@NotNull String column)
  {
    this.column = column;
  }

  @Override
  public boolean isValidRow(@NotNull Map<String, Object> row)
  {
    if (!row.containsKey(column)) {
      return false;
    }
    return inValues.contains(row.get(column));
  }

  @Override
  public boolean isValidJoin(@NotNull Map<String, Object> row1, @NotNull Map<String, Object> row2)
  {
    return false;
  }

  public String getColumn()
  {
    return column;
  }

  public void setColumn(String column)
  {
    this.column = column;
  }

  public void addInValue(Object value)
  {
    this.inValues.add(value);
  }

}
