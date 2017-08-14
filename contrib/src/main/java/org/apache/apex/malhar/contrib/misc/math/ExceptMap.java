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
package org.apache.apex.malhar.contrib.misc.math;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.algo.MatchMap;
import org.apache.apex.malhar.lib.util.UnifierHashMap;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;

/**
 * This operator does comparison on tuple sub-classed from Number based on the property "key", "value", and "cmp", and not matched tuples are emitted.
 * <p>
 * The comparison is done by getting double value from the Number. Both output ports
 * are optional, but at least one has to be connected
 * <p>
 * This module is a pass through<br>
 * <br>
 * <br>
 * StateFull : No, output is emitted in current window. <br>
 * Partitions : Yes, No state dependency among input tuples. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>except</b>: emits HashMap&lt;K,V&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq",
 * "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * <b>Compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt",
 * "gte"<br>
 * <br>
 * <b>Run time checks</b>:<br>
 * Does the incoming HashMap have the key, Is the value of the key a number<br>
 * <br>
 * @displayName Except Map
 * @category Math
 * @tags comparison, Number
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
@Stateless
public class ExceptMap<K, V extends Number> extends MatchMap<K, V>
{
        /**
         * Output port that emits non matching number tuples.
         */
  public final transient DefaultOutputPort<HashMap<K, V>> except = new DefaultOutputPort<HashMap<K, V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return new UnifierHashMap<K, V>();
    }
  };

  /**
   * Does nothing. Overrides base as call super.tupleMatched() would emit the
   * tuple
   *
   * @param tuple
   */
  @Override
  public void tupleMatched(Map<K, V> tuple)
  {
  }

  /**
   * Emits the tuple. Calls cloneTuple to get a copy, allowing users to override
   * in case objects are mutable
   *
   * @param tuple
   */
  @Override
  public void tupleNotMatched(Map<K, V> tuple)
  {
    except.emit(cloneTuple(tuple));
  }
}
