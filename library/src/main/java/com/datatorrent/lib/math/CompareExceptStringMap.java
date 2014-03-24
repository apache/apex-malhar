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
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.algo.MatchStringMap;
import com.datatorrent.lib.util.UnifierHashMap;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * A compare metric is done on String tuple based on the property "key", "value", and "cmp" both matching and non matching tuples on emitted on respective ports. If the tuple
 * passed the test, it is emitted on the output port "compare". If the tuple fails it is emitted on port "except". The comparison is done parsing a double
 * value from the String. Both output ports are optional, but at least one has to be connected<p>
 *  * This module is a pass through<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,String&gt;<br>
 * <b>compare</b>: emits HashMap&lt;K,String&gt;<br>
 * <b>except</b>: emits HashMap&lt;K,String&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * <b>Compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <b>Specific run time checks</b>:<br>
 * Does the incoming HashMap have the key<br>
 * Is the value of the key a number<br>
 * <br>
 *
 * @since 0.3.2
 */
public class CompareExceptStringMap<K> extends MatchStringMap<K>
{
  @OutputPortFieldAnnotation(name = "compare", optional=true)
  public final transient DefaultOutputPort<HashMap<K,String>> compare = match;

  @OutputPortFieldAnnotation(name = "except", optional=true)
  public final transient DefaultOutputPort<HashMap<K,String>> except = new DefaultOutputPort<HashMap<K,String>>()
  {
    @Override
    public Unifier<HashMap<K, String>> getUnifier()
    {
      return new UnifierHashMap<K, String>();
    }
  };

  /**
   * Emits if compare port is connected
   * @param tuple
   */
  @Override
  public void tupleMatched(Map<K,String> tuple)
  {
    if (compare.isConnected()) {
      compare.emit(cloneTuple(tuple));
    }
  }

  /**
   * Emits if except port is connected
   * @param tuple
   */
  @Override
  public void tupleNotMatched(Map<K,String> tuple)
  {
    if (except.isConnected()) {
      except.emit(cloneTuple(tuple));
    }
  }
}
