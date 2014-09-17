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
package com.datatorrent.lib.pigquery.condition;

import java.util.Map;

import javax.validation.constraints.NotNull;


/**
 * Group condition sub class to group tuples by column value.
 * <p>
 * @displayName: Pig Name Group Condition
 * @category: pigquery.condition
 * @tag: group, condition, map, string
 * @since 0.3.4
 */
public class PigNameGroupCondition implements PigGroupCondition
{
  /**
   * Group by key name.
   */
  @NotNull
  private String groupByName;

  public PigNameGroupCondition(@NotNull String groupByName) {
    this.groupByName = groupByName;
  }
  
  /* (non-Javadoc)
   * @see com.datatorrent.lib.pigquery.GroupByCondition#compute(java.util.Map)
   */
  @Override
  public Object compute(@NotNull Map<String, Object> tuple)
  {
    return tuple.get(groupByName);
  }

  /**
   * Get value for groupByName.
   * @return String
   */
  public String getGroupByName()
  {
    return groupByName;
  }

  /**
   * Set value for groupByName.
   * @param groupByName set value for groupByName.
   */
  public void setGroupByName(String groupByName)
  {
    this.groupByName = groupByName;
  }
}
