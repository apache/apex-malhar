/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseNumberValueOperator;

/**
 *
 * Emits the sum, and count of values at the end of window. <p>
 * This is an end of window operator<br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>sum</b>: emits V extends Number<br>
 * <b>count</b>: emits Integer</b>
 * <br>
 * <b>Properties</b>:<br>
 * <b>resetAtEndWindow</b>: If set to true sum, and count are calculated separately for each window.
 * <b> If set to false sum, and count are calculated spanned over all windows. Default value is true.<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Sum&lt;V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 500 Million tuples/s</b></td><td>One K,V or K,Integer tuples per window per port</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(V)</th><th><i>sum</i>(V)</th><th><i>count</i>(Integer)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>1000</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>10</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>52</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>22</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>14</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>1106</td><td>8</td></tr>
 * </table>
 * <br>
 *
 * @param <V>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class Sum<V extends Number> extends BaseNumberValueOperator<V>
{
  /**
   * Input port to receive data.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
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
  @OutputPortFieldAnnotation(name = "sum", optional = true)
  public final transient DefaultOutputPort<V> sum = new DefaultOutputPort<V>(this);
  @OutputPortFieldAnnotation(name = "count", optional = true)
  public final transient DefaultOutputPort<Integer> count = new DefaultOutputPort<Integer>(this);
  protected transient double sums = 0;
  protected transient int counts = 0;

  /**
   * Emits sum and count if ports are connected
   */
  @Override
  public void endWindow()
  {
    // May want to send out only if count != 0
    if (doSumEmit()) {
      sum.emit(getValue(sums));
    }
    if (doCountEmit()) {
      count.emit(new Integer(counts));
    }
    clearCache();

  }

  /**
   * Clears the cache making this operator stateless on window boundary
   */
  public void clearCache()
  {
    sums = 0;
    counts = 0;
  }

  /**
   * Decides whether emit has to be done in this window on port "sum"
   *
   * @return true is sum port is connected
   */
  public boolean doSumEmit()
  {
    return sum.isConnected();
  }

  /**
   * Decides whether emit has to be done in this window on port "count"
   *
   * @return true is sum port is connected
   */
  public boolean doCountEmit()
  {
    return count.isConnected();
  }
}
