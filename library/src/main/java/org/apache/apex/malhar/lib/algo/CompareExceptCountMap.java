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
package org.apache.apex.malhar.lib.algo;

import java.util.Map;

import org.apache.apex.malhar.lib.util.UnifierSumNumber;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator produces a count of how many tuples of value type Number satisfy and do not satisfy a specified compare function.
 * <p>
 * A count is done on how many tuples of value type Number satisfy the compare function. The function is given by
 * "key", "value", and "cmp". If a tuple passed the test count is incremented. On end of window count is emitted on the output port "count".
 * The comparison is done by getting double value from the Number.
 * </p>
 * <p>
 * This module is an end of window module. If no tuple comes in during a window 0 is emitted on both ports, thus no matter what one Integer
 * tuple is emitted on each port<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compare across application window(s). <br>
 * <b>Partitions : Yes, </b> count is unified at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>count</b>: emits Integer<br>
 * <b>except</b>: emits Integer<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * <b>Specific compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * </p>
 *
 * @displayName Compare Match and No Match Count
 * @category Rules and Alerts
 * @tags count, key value
 *
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
@OperatorAnnotation(partitionable = true)
public class CompareExceptCountMap<K, V extends Number> extends MatchMap<K, V>
{
  /**
   * The output port on which the number of tuples satisfying the compare function is emitted.
   */
  public final transient DefaultOutputPort<Integer> count = new DefaultOutputPort<Integer>()
  {
    @Override
    public Unifier<Integer> getUnifier()
    {
      return new UnifierSumNumber<Integer>();
    }
  };


  /**
   * The output port on which the number of tuples not satisfying the compare function is emitted.
   */
  public final transient DefaultOutputPort<Integer> except = new DefaultOutputPort<Integer>()
  {
    @Override
    public Unifier<Integer> getUnifier()
    {
      return new UnifierSumNumber<Integer>();
    }
  };


  protected int tcount = 0;
  protected int icount = 0;

  /**
   * Increments matched tuple count
   * @param tuple
   */
  @Override
  public void tupleMatched(Map<K, V> tuple)
  {
    tcount++;
  }

  /**
   * Increments not-matched tuple count
   * @param tuple
   */
  @Override
  public void tupleNotMatched(Map<K, V> tuple)
  {
    icount++;
  }

  /**
   * Emits the counts
   */
  @Override
  public void endWindow()
  {
    count.emit(tcount);
    except.emit(icount);
    tcount = 0;
    icount = 0;
  }
}
