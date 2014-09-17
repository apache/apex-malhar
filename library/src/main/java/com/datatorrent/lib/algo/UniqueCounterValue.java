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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This operator counts the number of unique tuples it recieved in a window and outputs it at the end of each window.
 * <p>
 * Counts the number of tuples emitted in a window.
 * </p>
 * <p>
 * This is an end of window operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>count</b>: emits Integer<br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for UniqueCounter&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; processes 110 Million K,V pairs/s</b></td><td>Emits one tuple per window</td><td>In-bound throughput
 * and number of unique k are the main determinant of performance. Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys,
 * the benchmarks may be lower</td></tr>
 * </table><br>
 * </p>
 * <p>
 * <b>Function Table (K=String)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for UniqueCounter&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(K)</th><th><i>count</i>(Integer)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>b</td></tr>
 * <tr><td>Data (process())</td><td>c</td></tr>
 * <tr><td>Data (process())</td><td>4</td></tr>
 * <tr><td>Data (process())</td><td>5ah</td></tr>
 * <tr><td>Data (process())</td><td>h</td></tr>
 * <tr><td>Data (process())</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>5ah</td></tr>
 * <tr><td>Data (process())</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>c</td></tr>
 * <tr><td>Data (process())</td><td>c</td></tr>
 * <tr><td>Data (process())</td><td>b</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>15</td></tr>
 * </table>
 * <br>
 * </p>
 *
 * @displayName Count Unique Values
 * @category algorithm
 * @tags count
 *
 * @since 0.3.2
 */
public class UniqueCounterValue<K> extends BaseOperator implements Unifier<Integer>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Counts tuples.
     */
    @Override
    public void process(K tuple)
    {
      counts++;
    }

  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<Integer> count = new DefaultOutputPort<Integer>()
  {
    @Override
    public Unifier<Integer> getUnifier()
    {
      return UniqueCounterValue.this;
    }

  };
  protected int counts = 0;

  /**
   * Emits total number of tuples.
   */
  @Override
  public void endWindow()
  {
    if (counts != 0) {
      count.emit(counts);
    }
    counts = 0;
  }

  @Override
  public void process(Integer tuple)
  {
    counts += tuple;
  }

}
