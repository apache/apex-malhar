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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberValueOperator;

/**
 *
 * Emits the change in the value in stream on port data (as compared to a base value set via port base) for every tuple. <p>
 * This is a pass through node. Tuples that arrive on base port are kept in cache forever<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>base</b>: expects V extends Number<br>
 * <b>change</b>: emits V extends Number<br>
 * <b>percent</b>: emits Double<br>
 * <br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for ChangeMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>28 million tuples/sec</b></td><td>Emits one value per input per port</td>
 * <td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for ChangeMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th colspan=2>In-bound (<i>data</i>::process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(V)</th><th><i>base</i>(V)</th><th><i>change</i>(V)</th><th><i>percent</i>(Double)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td></td><td>2</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>3</td><td></td><td>1</td><td>50.0</td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td><td>2</td><td>100.0</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * <br>
 */
public class Change<V extends Number> extends BaseNumberValueOperator<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Process each key, compute change or percent, and emit it.
     */
    @Override
    public void process(V tuple)
    {
      if (baseValue != 0) { // Avoid divide by zero, Emit an error tuple?
        double cval = tuple.doubleValue() - baseValue;
        change.emit(getValue(cval));
        percent.emit((cval / baseValue) * 100);
      }
    }
  };
  @InputPortFieldAnnotation(name = "base")
  public final transient DefaultInputPort<V> base = new DefaultInputPort<V>()
  {
    /**
     * Process each key to store the value. If same key appears again update with latest value.
     */
    @Override
    public void process(V tuple)
    {
      if (tuple.doubleValue() != 0.0) { // Avoid divide by zero, Emit an error tuple?
        baseValue = tuple.doubleValue();
      }
    }
  };
  // Default partition "pass through" works for change and percent, as it is done per tuple
  @OutputPortFieldAnnotation(name = "change", optional = true)
  public final transient DefaultOutputPort<V> change = new DefaultOutputPort<V>();
  @OutputPortFieldAnnotation(name = "percent", optional = true)
  public final transient DefaultOutputPort<Double> percent = new DefaultOutputPort<Double>();
  /**
   * baseValue is a stateful field. It is retained across windows.
   */
  private double baseValue = 0;
}
