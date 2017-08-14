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
package org.apache.apex.malhar.lib.streamquery.condition;


import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * An implementation of equal join condition class.
 * <p>
 * This compares values of given keys in both row data.
 * <br>
 * <b> Properties : </b> <br>
 * <b. equalkeys : </b> Keys for which value must be compared. <br>
 * <br>
 * @displayName Join Column Equal Condition
 * @category Stream Manipulators
 * @tags sql condition, equal join
 * @since 0.3.3
 */
public class JoinColumnEqualCondition  extends Condition
{

  /**
   * column names to be compared.
   */
  @NotNull
  private String column1;
  @NotNull
  private String column2;

  public JoinColumnEqualCondition(@NotNull String column1, @NotNull String column2)
  {
    this.column1 = column1;
    this.column2 = column2;
  }

  /**
   * Must never be called.
   */
  @Override
  public boolean isValidRow(Map<String, Object> row)
  {
    assert (false);
    return false;
  }

  /**
   *
   */
  @Override
  public boolean isValidJoin(Map<String, Object> row1, Map<String, Object> row2)
  {
    if (!row1.containsKey(column1) || !row2.containsKey(column2)) {
      return false;
    }
    Object value1 = row1.get(column1);
    Object value2 = row2.get(column2);
    return value1.equals(value2);
  }
}
