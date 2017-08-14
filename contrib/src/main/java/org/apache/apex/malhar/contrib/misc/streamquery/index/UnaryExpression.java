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

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.streamquery.index.Index;

/**
 * A base implementation of an index that filters row by unary expression.&nbsp; Subclasses should provide the
   implementation of filter/getExpressionName functions.
 * <p>
 * Sub class will implement filter/getExpressionName functions.
 * @displayName Unary Expression
 * @category Stream Manipulators
 * @tags unary, alias
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public abstract class UnaryExpression  implements Index
{
  /**
   * Column name argument for unary expression.
   */
  @NotNull
  protected String column;

  /**
   *  Alias name for output field.
   */
  protected String alias;

  /**
   * @param column name argument for unary expression.
   * @param alias name for output field.
   */
  public UnaryExpression(@NotNull String column, String alias)
  {
    this.column = column;
  }

  public String getColumn()
  {
    return column;
  }

  public void setColumn(String column)
  {
    this.column = column;
  }

  public String getAlias()
  {
    return alias;
  }

  public void setAlias(String alias)
  {
    this.alias = alias;
  }
}
