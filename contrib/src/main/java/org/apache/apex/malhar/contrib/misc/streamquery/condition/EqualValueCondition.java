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

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.streamquery.condition.Condition;

/**
 * An implementation of condition on column equality.
 * <p>
 * A valid row must have all key/value map in column name/value map.
 *
 * <b> Properties : </b> <br>
 *  <b> equalMap : </b> Column equal value map store.
 * @displayName Equal Value Condition
 * @category Stream Manipulators
 * @tags sql condition
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class EqualValueCondition extends Condition
{

  /**
   * Equal column map.
   */
  private HashMap<String, Object> equalMap = new HashMap<String, Object>();

  /**
   * Add column equal condition.
   */
  public void addEqualValue(String column, Object value)
  {
    equalMap.put(column, value);
  }

  /**
   * Check valid row.
   */
  @Override
  public boolean isValidRow(Map<String, Object> row)
  {
    // no conditions
    if (equalMap.size() == 0) {
      return true;
    }

    // compare each condition value
    for (Map.Entry<String, Object> entry : equalMap.entrySet()) {
      if (!row.containsKey(entry.getKey())) {
        return false;
      }
      Object value = row.get(entry.getKey());
      if (entry.getValue() == null) {
        if (value == null) {
          return true;
        }
        return false;
      }
      if (value == null) {
        return false;
      }
      if (!entry.getValue().equals(value)) {
        return false;
      }
    }
    return true;
  }

  /**
   * check valid join, not implemented
   *
   * @return false
   */
  @Override
  public boolean isValidJoin(Map<String, Object> row1, Map<String, Object> row2)
  {
    return false;
  }
}
