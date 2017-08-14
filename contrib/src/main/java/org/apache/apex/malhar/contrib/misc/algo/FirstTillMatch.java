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

import org.apache.apex.malhar.lib.util.BaseMatchOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator filters the incoming stream of key value pairs by obtaining the values corresponding to a specified key,
 * and comparing those values to a specified number.&nbsp;For each window, all key value pairs are emitted by the operator until a value satisfying the comparison is encountered.
 * <p>
 * All key.val pairs with val sub-classed from Number are emitted till the first match;  A compare metric is done based on the property "key",
 * "value", and "cmp". Then on no tuple is emitted in that window. The comparison is done by getting double value of the Number.
 * </p>
 * <p>
 * This module is a pass through<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are processed in current window. <br>
 * <b>Partitions : No, </b>will yield wrong results. <br>
 * <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input port, expects HashMap&lt;K,V&gt;<br>
 * <b>first</b>: Output port, emits HashMap&lt;K,V&gt; if compare function returns true<br>
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
 * @displayName Emit Keyval Pairs Until Match (Number)
 * @category Rules and Alerts
 * @tags filter, key value, numeric
 * @deprecated
 * @since 0.3.2
 */
@Deprecated
@OperatorAnnotation(partitionable = false)
public class FirstTillMatch<K, V extends Number> extends BaseMatchOperator<K, V>
{
  /**
   * Tuple emitted flag.
   */
  boolean emitted = false;

  /**
   * The input port on which incoming key value pairs are received.
   */
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>()
  {
    /**
     * Compares the key,val pair with the match condition. Till a match is found tuples are emitted.
     * Once a match is found, state is set to emitted, and no more tuples are compared (no more emits).
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      if (emitted) {
        return;
      }
      V val = tuple.get(getKey());
      if (val == null) { // skip if the key does not exist
        return;
      }
      if (compareValue(val.doubleValue())) {
        emitted = true;
      }
      if (!emitted) {
        first.emit(cloneTuple(tuple));
      }
    }
  };

  /**
   * The output port on which key value pairs are emitted until the first match.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> first = new DefaultOutputPort<HashMap<K, V>>();

  /**
   * Emitted set is reset to false
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    emitted = false;
  }
}
