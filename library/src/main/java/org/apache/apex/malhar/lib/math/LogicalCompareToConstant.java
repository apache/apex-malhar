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
package org.apache.apex.malhar.lib.math;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator does a logical comparison of a constant with a tuple.
 * <p>
 *
 * @see If the constant is equal to tuple, then the pair is
 *      emitted on equalTo, greaterThanEqualTo, and lessThanEqualTo ports. If
 *      the constant is less than tuple, then the pair is emitted on notEqualTo,
 *      lessThan and lessThanEqualTo ports. If the constant is greater than
 *      tuple, then the pair is emitted on notEqualTo, greaterThan and
 *      greaterThanEqualTo ports. This is a pass through operator
 *      <p>
 *      <br>
 *      StateFull : No, comparison is done in current window. <br>
 *      Partitions : Yes, no state dependency among input tuples. <br>
 *      <br>
 *      <b>Ports</b>:<br>
 *      <b>input</b>: expects T<br>
 *      <b>equalTo</b>: emits T<br>
 *      <b>notEqualTo</b>: emits T<br>
 *      <b>greaterThanEqualTo</b>: emits T<br>
 *      <b>greaterThan</b>: emits T<br>
 *      <b>lessThanEqualTo</b>: emits T<br>
 *      <b>lessThan</b>: emits T<br>
 *      <br>
 * @displayName Logical Compare To Constant
 * @category Math
 * @tags comparison, logical, key value, constant
 * @since 0.3.3
 */
@Stateless
public class LogicalCompareToConstant<T extends Comparable<? super T>> extends
    BaseOperator
{

  /**
   * Compare constant, set by application.
   */
  private T constant;

  /**
   * Input port that takes a comparable to compare it with a constant.
   */
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      int i = constant.compareTo(tuple);
      if (i > 0) {
        greaterThan.emit(tuple);
        greaterThanOrEqualTo.emit(tuple);
        notEqualTo.emit(tuple);
      } else if (i < 0) {
        lessThan.emit(tuple);
        lessThanOrEqualTo.emit(tuple);
        notEqualTo.emit(tuple);
      } else {
        equalTo.emit(tuple);
        lessThanOrEqualTo.emit(tuple);
        greaterThanOrEqualTo.emit(tuple);
      }
    }

  };

  /**
   * Equal output port.
   */
  public final transient DefaultOutputPort<T> equalTo = new DefaultOutputPort<T>();

  /**
   * Not Equal output port.
   */
  public final transient DefaultOutputPort<T> notEqualTo = new DefaultOutputPort<T>();

  /**
   * Less Than output port.
   */
  public final transient DefaultOutputPort<T> lessThan = new DefaultOutputPort<T>();

  /**
   * Greater than output port.
   */
  public final transient DefaultOutputPort<T> greaterThan = new DefaultOutputPort<T>();
  public final transient DefaultOutputPort<T> lessThanOrEqualTo = new DefaultOutputPort<T>();
  public final transient DefaultOutputPort<T> greaterThanOrEqualTo = new DefaultOutputPort<T>();

  /**
   * Set constant for comparison.
   *
   * @param constant
   *          the constant to set
   */
  public void setConstant(T constant)
  {
    this.constant = constant;
  }

  /**
   * returns the value of constant
   */
  public T getConstant()
  {
    return constant;
  }
}
