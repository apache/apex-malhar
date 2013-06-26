/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
import java.util.HashMap;
import java.util.Map;

/**
 *
 * A compare operation on a tuple with value type String, based on the property "key", "value", and "cmp"; the first match is emitted. The comparison is done by getting double
 * value from the Number.<p>
 * This module is a pass through<br>
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
 * <b>Specific compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * <b>Specific run time checks</b>: None <br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for FirstMatchStringMap&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 35 Million K,V pairs/s</b></td><td>Emits only 1 tuple per window</td><td>In-bound throughput and the occurrence of the
 * first match are the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String); keys=a; cmp=eq; value=3</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for FirstMatchStringMap&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,String&gt;)</th><th><i>first</i>(HashMap&lt;K,String&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=1000,b=-5}</td><td>{a=3,h=20,c=1000,b=-5}</td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=1,d=5,d=55}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=1,a=3,e=2}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * <br>
 *
 */
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
