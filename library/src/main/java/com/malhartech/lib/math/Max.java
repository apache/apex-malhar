/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.lib.util.BaseNumberValueOperator;

/**
 *
 * Emits at end of window maximum of all values sub-classed from Number in the incoming stream. <p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>max</b>: emits V<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Max&lt;V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 500 Million tuples/s</b></td><td>One tuple per window</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * <p>
 * <b>Function Table (V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Max&lt;V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(V)</th><th><i>max</i>(V)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>44</td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>1000</td><td></td></tr>
 * <tr><td>Data (process())</td><td>14</td><td></td></tr>
 * <tr><td>Data (process())</td><td>-10</td><td></td></tr>
 * <tr><td>Data (process())</td><td>52</td><td></td></tr>
 * <tr><td>Data (process())</td><td>22</td><td></td></tr>
 * <tr><td>Data (process())</td><td>14</td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>22</td><td></td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>1000</td></tr>
 * </table>
 * <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class Max<V extends Number> extends BaseNumberValueOperator<V> implements Unifier<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    /**
     * Each tuple is compared to the max and a new max (if so) is stored
     */
    @Override
    public void process(V tuple)
    {
      merge(tuple);
    }
  };

  @Override
  public void merge(V tuple)
  {
    if (!flag) {
      high = tuple.doubleValue();
      flag = true;
    }
    else if (high < tuple.doubleValue()) {
      high = tuple.doubleValue();
    }
  }

  @OutputPortFieldAnnotation(name = "max")
  public final transient DefaultOutputPort<V> max = new DefaultOutputPort<V>(this)
   {
    @Override
    public Unifier<V> getUnifier()
    {
      return Max.this;
    }
  };

  protected transient double high;
  protected transient boolean flag = false;

  /**
   * Node only works in windowed mode. Emits the max. Override getValue if tuple type is mutable
   * Clears internal data
   */
  @Override
  public void endWindow()
  {
    if (flag) {
      max.emit(getValue(high));
    }
    flag = false;
  }
}
