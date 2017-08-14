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

import org.apache.apex.malhar.lib.streamquery.index.ColumnIndex;

/**
 * <p>An implementation of column index that implements filter method using Round Double Index. </p>
 *
 * @displayName Round Double Index
 * @category Stream Manipulators
 * @tags alias, maths
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class RoundDoubleIndex  extends ColumnIndex
{
  private int rounder;
  public RoundDoubleIndex(@NotNull String column, String alias, int numDecimals)
  {
    super(column, alias);
    rounder = 1;
    if (numDecimals > 0) {
      rounder = (int)Math.pow(10, numDecimals);
    }
  }

  @Override
  public void filter(@NotNull  Map<String, Object> row, @NotNull  Map<String, Object> collect)
  {
    if (!row.containsKey(column)) {
      return;
    }
    double value = (Double)row.get(column);
    value = Math.round(value * rounder) / rounder;
    String name = getColumn();
    if (alias != null) {
      name = alias;
    }
    collect.put(name, value);
  }
}

