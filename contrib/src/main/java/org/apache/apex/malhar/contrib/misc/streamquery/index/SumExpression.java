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
package org.apache.apex.malhar.contrib.misc.streamquery.index;

import java.util.Map;

import javax.validation.constraints.NotNull;


/**
 * Implements sum on column index.
 * <p>
 * Select index class for implementing sum column index.
 * @displayName Sum Expression
 * @category Stream Manipulators
 * @tags sum
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class SumExpression extends BinaryExpression
{

  /**
   * @param left column name argument for expression.
   * @param right column name argument for expression.
   * @param alias name for output field.
   */
  public SumExpression(@NotNull String left, @NotNull String right, String alias)
  {
    super(left, right, alias);
    if (this.alias == null) {
      this.alias = "SUM(" + left + "," + right + ")";
    }
  }

  /* sum column values.
   * @see org.apache.apex.malhar.lib.streamquery.index.Index#filter(java.util.Map, java.util.Map)
   */
  @Override
  public void filter(Map<String, Object> row, Map<String, Object> collect)
  {
    if (!row.containsKey(left) || !row.containsKey(right)) {
      return;
    }
    collect.put(alias, ((Number)row.get(left)).doubleValue() + ((Number)row.get(right)).doubleValue());
  }

}
