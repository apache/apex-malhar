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
package org.apache.apex.malhar.lib.streamquery.index;

import java.util.Map;

import javax.validation.constraints.NotNull;


/**
 * <p>An implementation of an index that overrides filter method on basis on columns. </p>
 * <p>
 * @displayName Column Index
 * @category Stream Manipulators
 * @tags alias
 * @since 0.3.4
 */
public class ColumnIndex implements Index
{
  /**
   * Column/alias name.
   */
  protected String column = null;
  protected String alias = null;

  public ColumnIndex()
  {
  }

  public ColumnIndex(@NotNull String column, String alias)
  {
    this.setColumn(column);
    this.alias = alias;
  }

  @Override
  public void filter(@NotNull  Map<String, Object> row, @NotNull  Map<String, Object> collect)
  {
    String name = getColumn();
    if (alias != null) {
      name = alias;
    }
    collect.put(name, row.get(name));
  }

  public String getColumn()
  {
    return column;
  }

  public void setColumn(String column)
  {
    this.column = column;
  }
}
