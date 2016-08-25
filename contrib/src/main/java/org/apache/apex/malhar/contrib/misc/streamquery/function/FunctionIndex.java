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

/**
 * A base class for select aggregate function implementation.&nbsp; Subclasses should provide the
   implementation for aggregate compute functions.
 * <p>
 * <br>
 * <b>Properties : </b> <br>
 * <b>column : </b> Column name for aggregation.
 * <b>alias : </b> Output value alias name.
 * @displayName Function Index
 * @category Stream Manipulators
 * @tags sql aggregate
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public abstract class FunctionIndex
{
  /**
   * Column name.
   */
  @NotNull
  protected String column;

  /**
   * Alias name.
   */
  protected String alias;

  /**
   * @param column Column name for aggregation.
   * @param alias Output value alias name.
   */
  public FunctionIndex(@NotNull String column, String alias)
  {
    this.column = column;
    this.alias = alias;
  }

  /**
   * Aggregate compute function, implementation in sub class.
   * @param rows Tuple list over application window.
   * @return aggregate result object.
   */
  public abstract Object compute(@NotNull ArrayList<Map<String, Object>> rows) throws Exception;

  /**
   * Get aggregate output value name.
   * @return name string.
   */
  protected abstract String aggregateName();

  /**
   * Apply compute function to given rows and store result in collect by output value name.
   * @param  rows Tuple list over application window.
   */
  public void filter(ArrayList<Map<String, Object>> rows, Map<String, Object> collect) throws Exception
  {
    if (rows == null) {
      return;
    }
    String name = column;
    if (alias != null) {
      name = alias;
    }
    if (name == null) {
      name = aggregateName();
    }
    collect.put(name, compute(rows));
  }
}
