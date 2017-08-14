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
 * This operator filters the incoming stream of key value pairs by obtaining the values corresponding to a specified key,
 * and comparing those values to a specified value.&nbsp;The last key value pair, in each window, to satisfy the comparison is emitted.
 * <p>
 * A compare function is  operated on a tuple value sub-classed from Number based on the property "key", "value", and "cmp". Every tuple
 * is checked and the last one that passes the condition is send during end of window on port "last". The comparison is done by getting double
 * value from the Number.
 * </p>
 * <p>
 * This module is an end of window module<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compare across application window(s). <br>
 * <b>Partitions : No, </b> will yield wrong result. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>last</b>: emits Map&lt;K,V&gt; in end of window for the last tuple on which the compare function is true<br>
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
 * @displayName Emit Last Match (Number)
 * @category Rules and Alerts
 * @tags filter, key value, numeric
 * @deprecated
 * @since 0.3.2
 */
@Deprecated
@OperatorAnnotation(partitionable = false)
public class LastMatchMap<K, V extends Number> extends BaseMatchOperator<K,V>
{
  /**
   * Last tuple.
   */
  protected HashMap<K, V> ltuple = null;

  /**
   * The input port on which key value pairs are received.
   */
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Processes tuples and keeps a copy of last matched tuple
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      V val = tuple.get(getKey());
      if (val == null) {
        return;
      }
      if (compareValue(val.doubleValue())) {
        ltuple = cloneTuple(tuple);
      }
    }
  };

  /**
   * The output port on which the last key value pair to satisfy the comparison function is emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> last = new DefaultOutputPort<HashMap<K, V>>();

  /**
   * Emits last matching tuple
   */
  @Override
  public void endWindow()
  {
    if (ltuple != null) {
      last.emit(ltuple);
    }
    ltuple = null;
  }
}
