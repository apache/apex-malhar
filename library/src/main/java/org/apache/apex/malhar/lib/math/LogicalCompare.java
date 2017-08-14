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
import com.datatorrent.common.util.Pair;

/**
 * This operator compares the two values in a given pair &lt;T,T&gt; object which are of the comparable property, and emits the pair on appropriate port denoting the result of the comparison.
 * <p>
 * If the first value is equal to second value, then the pair is emitted on equalTo, greaterThanEqualTo, and lessThanEqualTo ports.
 * If the first value is less than second value, then the pair is emitted on notEqualTo, lessThan and lessThanEqualTo ports.
 * If the first value is greater than second value, then the pair is emitted on notEqualTo, greaterThan and greaterThanEqualTo ports.
 * This is a pass through operator.
 * <br>
 * StateFull : No, output is computed during current window. <br>
 * Partitions : Yes, no dependency among input tuples. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Pair&lt;T,T&gt;<br>
 * <b>equalTo</b>: emits Pair&lt;T,T&gt;<br>
 * <b>notEqualTo</b>: emits Pair&lt;T,T&gt;<br>
 * <b>greaterThanEqualTo</b>: emits Pair&lt;T,T&gt;<br>
 * <b>greaterThan</b>: emits Pair&lt;T,T&gt;<br>
 * <b>lessThanEqualTo</b>: emits Pair&lt;T,T&gt;<br>
 * <b>lessThan</b>: emits Pair&lt;T,T&gt;<br>
 * <br>
 * @displayName Logical Compare
 * @category Math
 * @tags comparison, logical, key value
 * @since 0.3.3
 */
@Stateless
public abstract class LogicalCompare<T extends Comparable<? super T>> extends
    BaseOperator
{
  /**
   * Input port that takes a key, value pair for comparison.
   */
  public final transient DefaultInputPort<Pair<T, T>> input = new DefaultInputPort<Pair<T, T>>()
  {
    @Override
    public void process(Pair<T, T> tuple)
    {
      int i = tuple.first.compareTo(tuple.second);
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
  public final transient DefaultOutputPort<Pair<T, T>> equalTo = new DefaultOutputPort<Pair<T, T>>();

  /**
   * Not Equal output port.
   */
  public final transient DefaultOutputPort<Pair<T, T>> notEqualTo = new DefaultOutputPort<Pair<T, T>>();

  /**
   * Less than output port.
   */
  public final transient DefaultOutputPort<Pair<T, T>> lessThan = new DefaultOutputPort<Pair<T, T>>();

  /**
   * Greater than output port.
   */
  public final transient DefaultOutputPort<Pair<T, T>> greaterThan = new DefaultOutputPort<Pair<T, T>>();

  /**
   * Less than equal to output port.
   */
  public final transient DefaultOutputPort<Pair<T, T>> lessThanOrEqualTo = new DefaultOutputPort<Pair<T, T>>();

  /**
   * Greater than equal to output port.
   */
  public final transient DefaultOutputPort<Pair<T, T>> greaterThanOrEqualTo = new DefaultOutputPort<Pair<T, T>>();
}
