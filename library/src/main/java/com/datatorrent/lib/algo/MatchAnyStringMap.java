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

import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseMatchOperator;
import com.datatorrent.lib.util.UnifierBooleanOr;

/**
 * This operator filters the incoming stream of key value pairs by obtaining the values corresponding to a specified key,
 * and comparing those values to a specified number.&nbsp;
 * If the comparison returns true for any of the key value pairs within a window,
 * then a true is emitted at the end of the window.&nbsp;Otherwise a false is emitted at the end of the window.
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
 * <b>data</b>: expects Map<K,String><br>
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
 * <br>
 * </p>
 *
 * @displayName Emit Boolean For Match (String)
 * @category Algorithmic
 * @tags filter, key value, string
 *
 * @since 0.3.2
 */

@OperatorAnnotation(partitionable = true)
public class MatchAnyStringMap<K> extends BaseMatchOperator<K, String>
{
  /**
   * The input port which receives key value pairs.
   */
  public final transient DefaultInputPort<Map<K, String>> data = new DefaultInputPort<Map<K, String>>()
  {
    /**
     * Emits true on first matching tuple
     */
    @Override
    public void process(Map<K, String> tuple)
    {
      if (result) {
        return;
      }
      String val = tuple.get(getKey());
      if (val == null) { // skip if key does not exist
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
      result = !errortuple && compareValue(tvalue);
      if (result) {
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

  /**
   * Resets match flag
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    result = false;
  }
}
