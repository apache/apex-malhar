/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.streamquery.condition;

import java.util.ArrayList;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.streamquery.function.FunctionIndex;

/**
 *  A base class for Group,Having operator with aggregate index constraint.&nsbsp; Subclasses should provide the 
    implementation to check if aggregate is valid.
 * <p>
 * @displayName Having Condition
 * @category Streamquery/Condition
 * @tags sql condition, index, group
 * @since 0.3.4
 */
public abstract class HavingCondition
{
  /**
   * Aggregate index to be validated.
   */
  protected FunctionIndex  aggregateIndex = null;
  
  /**
   * @param aggregateIndex  Aggregate index to be validated.
   */
  public HavingCondition(FunctionIndex  aggregateIndex) {
    this.aggregateIndex = aggregateIndex;
  }
  
  /**
   *  Check if aggregate is valid.
   */
  abstract public boolean isValidAggregate(@NotNull ArrayList<Map<String, Object>> rows) throws Exception;
}
