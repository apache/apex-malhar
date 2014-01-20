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
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseMatchOperator;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * A compare function is  operated on a tuple value sub-classed from Number based on the property "key", "value", and "cmp". Every tuple
 * is checked and the last one that passes the condition is send during end of window on port "last". The comparison is done by getting double
 * value from the Number<p>
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
 *
 * @since 0.3.2
 */
@OperatorAnnotation(partitionable = false)
public class LastMatchMap<K, V extends Number> extends BaseMatchOperator<K,V>
{
  /**
   * Last tuple.
   */
  protected HashMap<K, V> ltuple = null;
  
  /**
   * Input port.
   */
  @InputPortFieldAnnotation(name = "data")
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
   * Output port.
   */
  @OutputPortFieldAnnotation(name = "last")
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
