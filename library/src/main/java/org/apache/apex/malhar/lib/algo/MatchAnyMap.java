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

import org.apache.apex.malhar.lib.util.BaseMatchOperator;
import org.apache.apex.malhar.lib.util.UnifierBooleanOr;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator filters the incoming stream of key value pairs by obtaining the values corresponding to a specified key,
 * and comparing those values to a specified number.&nbsp;
 * If the comparison returns true for any of the key value pairs within a window,
 * then a true is emitted at the end of the window.&nbsp;
 * Otherwise a false is emitted at the end of the window.
 * <p>
 * Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If any tuple passes a Boolean(true) is emitted, else a Boolean(false) is emitted on the output port "any".
 * The comparison is done by getting double value from the Number.
 * </p>
 * <p>
 * This module is a pass through as it emits the moment the condition is met<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compared across application window(s). <br>
 * <b>Partitions : Yes, </b> match status is unified on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>any</b>: emits Boolean<br>
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
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * </p>
 *
 * @displayName Emit Boolean For Match (Number)
 * @category Rules and Alerts
 * @tags filter, key value
 *
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
@OperatorAnnotation(partitionable = true)
public class MatchAnyMap<K, V extends Number> extends BaseMatchOperator<K,V>
{
  /**
   * The input port which receives key value pairs.
   */
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Emits true on first matching tuple
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      if (result) {
        return;
      }
      V val = tuple.get(getKey());
      if ((val != null) && compareValue(val.doubleValue())) {
        result = true;
        any.emit(true);
      }
    }
  };

  /**
   * The output port that emits true at the end of an application window if any tuple satisfies the comparison.
   */
  public final transient DefaultOutputPort<Boolean> any = new DefaultOutputPort<Boolean>()
  {
    @Override
    public Unifier<Boolean> getUnifier()
    {
      return new UnifierBooleanOr();
    }
  };

  protected boolean result = false;

  @Override
  public void beginWindow(long windowId)
  {
    result = false;
  }
}
