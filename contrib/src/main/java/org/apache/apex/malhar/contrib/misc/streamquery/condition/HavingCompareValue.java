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

import java.util.ArrayList;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.contrib.misc.streamquery.function.FunctionIndex;

/**
 *  A derivation of HavingCondition that implements comparison of aggregate index value to input compare value. <br>
 * <p>
 * Compare value must implement interface Comparable. <br>
 * <br>
 * <b> Properties : </b>
 *  <b> compareValue : </b>  Value to be compared. <br>
 *  <b>  compareType : </b> Type of comparison -1 == lt, 0 == eq, 1 == gt. <br>
 * @displayName Having Compare Value
 * @category Stream Manipulators
 * @tags compare, sql condition
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
@SuppressWarnings("rawtypes")
public class HavingCompareValue<T extends Comparable> extends HavingCondition
{
  /**
   * Value to be compared.
   */
  private T compareValue;

  /**
   * Type of comparison -1 == lt, 0 == eq, 1 == gt.
   */
  private int compareType;

  /**
   * @param aggregateIndex   aggregate index for comparison. <br>
   * @param compareValue     Value to be compared. <br>
   * @param compareType    Type of comparison -1 == lt, 0 == eq, 1 == gt. <br>
   */
  public HavingCompareValue(FunctionIndex aggregateIndex, T compareValue, int compareType)
  {
    super(aggregateIndex);
    this.compareValue = compareValue;
    this.compareType  = compareType;
  }

  /**
   * Validate aggregate override. <br>
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean isValidAggregate(@NotNull ArrayList<Map<String, Object>> rows) throws Exception
  {
    Object computed = aggregateIndex.compute(rows);
    return (compareType == compareValue.compareTo(computed));
  }

}
