/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberValueOperator;

/**
 *
 * Emits at end of window minimum of all values sub-classed from Number in the incoming stream. <p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>min</b>: emits V extends Number<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Min&lt;V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 500 Million tuples/s</b></td><td>One Double tuple per window</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower.</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Min&lt;V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(V)</th><th><i>min</i>(V)</th></tr>
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
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>-10</td></tr>
 * </table>
 * <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class Min<V extends Number> extends BaseNumberValueOperator<V> implements Unifier<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    /**
     * Each tuple is compared to the min and a new min (if so) is stored.
     */
    @Override
    public void process(V tuple)
    {
      Min.this.process(tuple);
    }
  };

  @Override
  public void process(V tuple)
  {
    if (!flag) {
      low = tuple;
      flag = true;
    }
    else if (low.doubleValue() > tuple.doubleValue()) {
      low = tuple;
    }
  }

  @OutputPortFieldAnnotation(name = "min")
  public final transient DefaultOutputPort<V> min = new DefaultOutputPort<V>(this)
  {
    @Override
    public Unifier<V> getUnifier()
    {
      return Min.this;
    }
  };

  protected V low;
  protected boolean flag = false;

  /**
   * Emits the max. Override getValue if tuple type is mutable.
   * Clears internal data. Node only works in windowed mode.
   */
  @Override
  public void endWindow()
  {
    if (flag) {
      min.emit(low);
    }
    flag = false;
    low = null;
  }
}
