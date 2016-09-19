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
 * <p> An implementation of sql sum function. </p>
 * <p>
 * @displayName Sum Function
 * @category Stream Manipulators
 * @tags sql sum, aggregate
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class SumFunction extends FunctionIndex
{
  public SumFunction(String column, String alias) throws Exception
  {
    super(column, alias);
  }

  @Override
  public Object compute(@NotNull ArrayList<Map<String, Object>> rows) throws Exception
  {
    Double result = 0.0;
    for (Map<String, Object> row : rows) {
      if (!row.containsKey(column)) {
        continue;
      }
      result += ((Number)row.get(column)).doubleValue();
    }
    return result;
  }

  @Override
  protected String aggregateName()
  {
    return "Sum(" + column;
  }

}
