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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.streamquery.condition.Condition;

/**
 * An implementation of condition class to filter rows for which given column name value matches given regular expression. <br>
 *<p>
 *<b> Properties : </b> <br>
 *<b> column : < /b> Column to be matched with regular expression. <br>
 *<b> pattern : </b> Regular expression pattern.<br>
 * @displayName Like Condition
 * @category Stream Manipulators
 * @tags sql, like condition, regular expression
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class LikeCondition extends Condition
{
  /**
   * Column to be matched with regular expression.
   */
  @NotNull
  private String column;

  /**
   * Regular expression pattern.
   */
  @NotNull
  private Pattern pattern;

  /**
   * @param column Column to be matched with regular expression, must be non-null.
   * @param pattern Regular expression pattern, must be non-null.
   */
  public LikeCondition(@NotNull String column,@NotNull String pattern)
  {
    setColumn(column);
    setPattern(pattern);
  }

  /**
   * For valid row column value string must match regular expression.
   * @return row valid status.
   */
  @Override
  public boolean isValidRow(Map<String, Object> row)
  {
    if (!row.containsKey(column)) {
      return false;
    }
    Matcher match = pattern.matcher((CharSequence)row.get(column));
    return match.find();
  }

  /**
   * Must not be called.
   */
  @Override
  public boolean isValidJoin(Map<String, Object> row1, Map<String, Object> row2)
  {
    assert (false);
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

  public void setPattern(String pattern)
  {
    this.pattern = Pattern.compile(pattern);
  }

}
