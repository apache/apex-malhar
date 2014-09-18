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
import com.datatorrent.api.annotation.*;
import com.datatorrent.lib.util.BaseMatchOperator;
import com.datatorrent.lib.util.UnifierHashMap;
import java.util.HashMap;
import java.util.Map;

/**
 * This operator filters the incoming stream of key value pairs by obtaining the values corresponding to a specified key,
 * and comparing those values to a specified number.&nbsp;
 * If a key value pair satisfies the comparison, then it is emitted.
 * <p>
 * A compare function is imposed based on the property "key", "value", and "cmp". If the tuple
 * passed the test, it is emitted on the output port "match". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected.
 * </p>
 * <p>
 * This module is a pass through<br>
 * <br>
 * <b>StateFull : No, </b> tuple is processed in current application window. <br>
 * <b>Partitions : Yes, </b> match status is unified on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,String&gt;<br>
 * <b>match</b>: emits HashMap&lt;K,String&gt; if compare function returns true<br>
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
 * @displayName Emit Matching Keyval Pairs (String)
 * @category algorithm
 * @tags filter, key value, string
 *
 * @since 0.3.2
 */
@Stateless
@OperatorAnnotation(partitionable = true)
public class MatchStringMap<K> extends BaseMatchOperator<K,String>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, String>> data = new DefaultInputPort<Map<K, String>>()
  {
    /**
     * Matchs tuple with the value and calls tupleMatched and tupleNotMatched based on if value matches
     */
    @Override
    public void process(Map<K, String> tuple)
    {
      String val = tuple.get(getKey());
      if (val == null) { // skip this tuple
        if (emitError) {
          tupleNotMatched(tuple);
        }
        return;
      }
      double tvalue = 0;
      boolean errortuple = false;
      try {
        tvalue = Double.parseDouble(val.toString());
      }
      catch (NumberFormatException e) {
        errortuple = true;
      }
      if (!errortuple) {
        if (compareValue(tvalue)) {
          tupleMatched(tuple);
        }
        else {
          tupleNotMatched(tuple);
        }
      }
      else if (emitError) {
        tupleNotMatched(tuple);
      }
    }
  };

  @OutputPortFieldAnnotation(name = "match", optional=true)
  public final transient DefaultOutputPort<HashMap<K, String>> match = new DefaultOutputPort<HashMap<K, String>>()
  {
    @Override
    public Unifier<HashMap<K, String>> getUnifier()
    {
      return new UnifierHashMap<K, String>();
    }
  };

  boolean emitError = true;

  /**
   * getter function for emitError flag.<br>
   * Error tuples (no key; val not a number) are emitted if this flag is true. If false they are simply dropped
   * @return emitError
   */
  public boolean getEmitError()
  {
    return emitError;
  }

  /**
   * setter funtion for emitError flag
   * @param val
   */
  public void setEmitError(boolean val)
  {
    emitError = val;
  }

  /**
   * Emits tuple if it. Call cloneTuple to allow users who have mutable objects to make a copy
   * @param tuple
   */
  public void tupleMatched(Map<K, String> tuple)
  {
    match.emit(cloneTuple(tuple));
  }

  /**
   * Does not emit tuple, an empty call. Sub class can override
   * @param tuple
   */
  public void tupleNotMatched(Map<K, String> tuple)
  {
  }
}
