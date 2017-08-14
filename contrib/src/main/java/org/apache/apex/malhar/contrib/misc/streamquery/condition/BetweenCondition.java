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

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.streamquery.condition.Condition;

/**
 *  A derivation of Condition that validates row by checking if the given column name value lies between given left,right range. <br>
 * <p>
 * <b>Properties : </b> <br>
 * <b> column : </b> Name of column. <br>
 * <b> leftValue : </b> left range of column value. <br>
 * <b> rightValue : </b> right range od column value. <br>
 * <br>
 * @displayName Between Condition
 * @category Stream Manipulators
 * @tags sql condition
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class BetweenCondition extends Condition
{
  /**
   * Column name to be checked.
   */
  @NotNull
  private String column;

  /**
   * Left range value.
   */
  @NotNull
  private Object leftValue;

  /**
   * Right range value.
   */
  @NotNull
  private Object rightValue;

  /**
   * @param  column  Name of column, must be non null. <br>
   * @param  leftValue  Left range for value, mut be non null. <br>
   * @param  rightValue  right range for value, mut be non null. <br>
   */
  public BetweenCondition(@NotNull String column, @NotNull  Object leftValue, @NotNull Object rightValue)
  {
    this.column = column;
    this.leftValue = leftValue;
    this.rightValue = rightValue;
  }

  /**
   * Validate given row.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public boolean isValidRow(@NotNull Map<String, Object> row)
  {
    if (!row.containsKey(column)) {
      return false;
    }
    Object value = row.get(column);
    if (value == null) {
      return false;
    }
    if (((Comparable)value).compareTo((Comparable)leftValue) < 0) {
      return false;
    }
    if (((Comparable)value).compareTo((Comparable)rightValue) > 0) {
      return false;
    }
    return true;
  }

  /**
   * Must not be called.
   */
  @Override
  public boolean isValidJoin(@NotNull Map<String, Object> row1, Map<String, Object> row2)
  {
    assert (false);
    return false;
  }

}
