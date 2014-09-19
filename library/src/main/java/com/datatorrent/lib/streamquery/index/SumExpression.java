/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.streamquery.index;

import java.util.Map;

import javax.validation.constraints.NotNull;


/**
 * Implements sum on column index. 
 * <p>
 * Select index class for implementing sum column index.
 * @displayName: Sum Expression
 * @category: streamquery/index
 * @tag: sum
 * @since 0.3.4
 */
public class SumExpression extends BinaryExpression
{

  /**
   * @param Left column name argument for expression.
   * @param Right column name argument for expression.
   * @param Alias name for output field.
   */
  public SumExpression(@NotNull String left, @NotNull String right, String alias)
  {
    super(left, right, alias);
    if (this.alias == null) this.alias = "SUM(" + left + "," + right + ")";
  }

  /* sum column values.
   * @see com.datatorrent.lib.streamquery.index.Index#filter(java.util.Map, java.util.Map)
   */
  @Override
  public void filter(Map<String, Object> row, Map<String, Object> collect)
  {
    if (!row.containsKey(left) || !row.containsKey(right)) return;
    collect.put(alias, ((Number)row.get(left)).doubleValue() + ((Number)row.get(right)).doubleValue());
  }

}
