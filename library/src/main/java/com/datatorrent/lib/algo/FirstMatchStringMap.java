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

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseMatchOperator;

/**
 *
 * A compare metric on a tuple with value type String, based on the property "key", "value", and "cmp"; the first match is emitted. The comparison is done by getting double
 * value from the Number.<p>
 * This module is a pass through<br>
 * <br>
 * <b>StateFull : No, </b> tuple are processed in current window. <br>
 * <b>Partitions : No, </b>will yield wrong results. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,String&gt;><br>
 * <b>first</b>: emits HashMap&lt;K,String&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 *
 * @since 0.3.2
 */
@OperatorAnnotation(partitionable = false)
public class FirstMatchStringMap<K> extends BaseMatchOperator<K,String>  
{

  @InputPortFieldAnnotation(name="data")
  public final transient DefaultInputPort<Map<K, String>> data = new DefaultInputPort<Map<K, String>>()
  {
    /**
     * Checks if required key,val pair exists in the HashMap. If so tuple is emitted, and emitted flag is set
     * to true
     */
    @Override
    public void process(Map<K, String> tuple)
    {
      if (emitted) {
        return;
      }
      String val = tuple.get(getKey());
      if (val == null) { // error tuple? skip if val is null
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
          first.emit(cloneTuple(tuple));
          emitted = true;
        }
      }
      else { // emit error tuple, the string has to be convertible to Double
      }
    }
  };

  @OutputPortFieldAnnotation(name="first")
  public final transient DefaultOutputPort<HashMap<K, String>> first = new DefaultOutputPort<HashMap<K, String>>();
  boolean emitted = false;

  /**
   * Resets emitted flag to false
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    emitted = false;
  }
}
