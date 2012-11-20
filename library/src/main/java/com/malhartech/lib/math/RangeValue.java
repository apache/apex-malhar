/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.lib.util.BaseNumberValueOperator;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableDouble;
import java.util.ArrayList;

/**
 *
 * Emits the range of values at the end of window<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>range</b>: emits ArrayList&lt;V&gt;(2); .get(0) gives max; .get(1) gives min<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for RangeValue&lt;V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 500 Million tuples/s</b></td><td>One ArrayList&lt;V&gt;(2) tuple per window</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for RangeValue&lt;V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i> (&lt;V&gt;)</th><th><i>range</i> (ArrayList&lt;V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>1000</td><td></td></tr>
 * <tr><td>Data (process())</td><td>10</td><td></td></tr>
 * <tr><td>Data (process())</td><td>52</td><td></td></tr>
 * <tr><td>Data (process())</td><td>22</td><td></td></tr>
 * <tr><td>Data (process())</td><td>14</td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{1000,2}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class RangeValue<V extends Number> extends BaseNumberValueOperator<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    /**
     * Process each tuple to compute new high and low
     */
    @Override
    public void process(V tuple)
    {
      if (low == null) {
        low = new MutableDouble(tuple.doubleValue());
      }
      else if (low.value > tuple.doubleValue()) {
        low.value = tuple.doubleValue();
      }

      if (high == null) {
        high = new MutableDouble(tuple.doubleValue());
      }
      else if (high.value < tuple.doubleValue()) {
        high.value = tuple.doubleValue();
      }
    }
  };

  @OutputPortFieldAnnotation(name = "range")
  public final transient DefaultOutputPort<ArrayList<V>> range = new DefaultOutputPort<ArrayList<V>>(this);
  MutableDouble high = null;
  MutableDouble low = null;

  /**
   * Clears the values
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    high = null;
    low = null;
  }

  /**
   * Emits the range. If no tuple was received in the window, no emit is done
   */
  @Override
  public void endWindow()
  {
    if ((low != null) && (high != null)) {
      ArrayList<V> tuple = new ArrayList<V>(2);
      tuple.add(getValue(high.value));
      tuple.add(getValue(low.value));
      range.emit(tuple);
    }
  }
}
