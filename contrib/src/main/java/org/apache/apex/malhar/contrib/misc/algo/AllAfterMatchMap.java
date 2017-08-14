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
package org.apache.apex.malhar.contrib.misc.algo;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.util.BaseMatchOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator takes Maps, whose values are numbers, as input tuples.&nbsp;
 * It then performs a numeric comparison on the values corresponding to one of the keys in the input tuple maps.&nbsp;
 * All tuples processed by the operator before the first successful comparison are not output by the operator,
 * all tuples processed by the operator after and including a successful comparison are output by the operator.
 *
 * <p>
 * A compare metric is done on input tuple based on the property "key",
 * "value", and "cmp" type. All tuples are emitted (inclusive) once a match is made.
 * The comparison is done by getting double value from the Number.
 * This module is a pass through<br>
 * <br>
 * <b> StateFull : Yes, </b> Count is aggregated over application window(s). <br>
 * <b> Partitions : No, </b> will yield wrong result. <br>
 * <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>allafter</b>: emits Map&lt;K,V extends Number&gt; if compare function
 * returns true<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq",
 * "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * <b>Specific compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt",
 * "gte"<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * </p>
 *
 * @displayName Emit All After Match (Number)
 * @category Rules and Alerts
 * @tags filter, compare, numeric, key value
 *
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
@OperatorAnnotation(partitionable = false)
public class AllAfterMatchMap<K, V extends Number> extends
    BaseMatchOperator<K, V>
{
  /**
   * The input port on which tuples are received.
   */
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Process HashMap<K,V> and emit all tuples at and after match
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      if (doemit) {
        allafter.emit(cloneTuple(tuple));
        return;
      }
      V v = tuple.get(getKey());
      if (v == null) { // error tuple
        return;
      }
      if (compareValue(v.doubleValue())) {
        doemit = true;
        allafter.emit(cloneTuple(tuple));
      }
    }
  };

  /**
   * The output port on which all tuples after a match are emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> allafter = new DefaultOutputPort<HashMap<K, V>>();
  boolean doemit = false;

  /**
   * Resets the matched variable
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    doemit = false;
  }
}
