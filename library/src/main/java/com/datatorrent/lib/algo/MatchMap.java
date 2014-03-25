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
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseMatchOperator;
import com.datatorrent.lib.util.UnifierHashMap;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * A compare function is imposed based on the property "key", "value", and "cmp". If the tuple
 * passed the test, it is emitted on the output port match. The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<p>
 * This module is a pass through<br>
 * <br>
 * <b>StateFull : No, </b> tuple is processed in current application window. <br>
 * <b>Partitions : Yes, </b> match status is unified on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>match</b>: emits HashMap&lt;K,V&gt<br>
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
public class MatchMap<K,V extends Number> extends BaseMatchOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * If tuple matches, tupleMatched is called, if not tupleNotMatched is called
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      V v = tuple.get(getKey());
      if (v == null) { // skip this tuple
        tupleNotMatched(tuple);
        return;
      }
      if (compareValue(v.doubleValue())) {
        tupleMatched(tuple);
      }
      else {
        tupleNotMatched(tuple);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "match", optional=true)
  public final transient DefaultOutputPort<HashMap<K, V>> match = new DefaultOutputPort<HashMap<K, V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return new UnifierHashMap<K, V>();
    }
  };


  /**
   * Emits tuple if it. Call cloneTuple to allow users who have mutable objects to make a copy
   * @param tuple
   */
  public void tupleMatched(Map<K, V> tuple)
  {
    match.emit(cloneTuple(tuple));
  }

  /**
   * No metric is done. Sub-classes can override and customize as needed
   * @param tuple
   */
  public void tupleNotMatched(Map<K, V> tuple)
  {
  }
}
