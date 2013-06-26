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
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberValueOperator;

/**
 *
 * Emits the average of values at the end of window. <p>
 * This is an end window operator. This can not be partitioned. Partitioning this will yield incorrect result.<br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>average</b>: emits V extends Number<br><br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for SumValue&lt;V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 175 Million tuples/s</b></td><td>Average value of tuples per window per port</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower.</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(V)</th><th><i>average</i>(V)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>1</td><td></td></tr>
 * <tr><td>Data (process())</td><td>10</td><td></td></tr>
 * <tr><td>Data (process())</td><td>9</td><td></td></tr>
 * <tr><td>Data (process())</td><td>22</td><td></td></tr>
 * <tr><td>Data (process())</td><td>14</td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>20</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>10</td></tr>
 * </table>
 * <br>
 *
 * <br>
 */
public class Average<V extends Number> extends BaseNumberValueOperator<V>
{
  /**
   * Input port
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(V tuple)
    {
      sums += tuple.doubleValue();
      counts++;
    }
  };

  /**
   * Output port
   */
  @OutputPortFieldAnnotation(name = "average")
  public final transient DefaultOutputPort<V> average = new DefaultOutputPort<V>();

  protected double sums = 0;
  protected long counts = 0;

  /**
   * Emit average.
   */
  @Override
  public void endWindow()
  {
    // May want to send out only if count != 0
    if (counts != 0) {
      average.emit(getAverage());
    }
    sums = 0;
    counts = 0;
  }

  /**
   * Calculate average based on number type.
   */
  public V getAverage()
  {
    if (counts == 0) {
      return null;
    }
    V num = getValue(sums);
    Number val;
    switch (getType()) {
      case DOUBLE:
        val = new Double(num.doubleValue()/counts);
        break;
      case INTEGER:
        int icount = (int)(num.intValue()/counts);
        val = new Integer(icount);
        break;
      case FLOAT:
        val = new Float(num.floatValue()/counts);
        break;
      case LONG:
        val = new Long(num.longValue()/counts);
        break;
      case SHORT:
        short scount = (short) (num.shortValue()/counts);
        val = new Short(scount);
        break;
      default:
        val = new Double(num.doubleValue()/counts);
        break;
    }
    return (V) val;
  }
}
