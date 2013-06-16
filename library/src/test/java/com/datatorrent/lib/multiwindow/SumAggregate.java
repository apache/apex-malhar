/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.multiwindow;

import com.malhartech.lib.math.Sum;

/**
 *
 * Emits the sum, average, and count of values at the end of window. <p>
 * This is an end of window operator<br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>sum</b>: emits V extends Number<br>
 * <b>count</b>: emits Integer</b>
 * <b>average</b>: emits V extends Number<br><br>
 * <br>
 * <b>Properties</b>: None<br>
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
 * <tr><th><i>data</i>(V)</th><th><i>sum</i>(V)</th><th><i>count</i>(Integer)</th><th><i>average</i>(V)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>1000</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>10</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>52</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>22</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>14</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>1106</td><td>8</td><td>138</td></tr>
 * </table>
 * <br>
 * @param <V>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class SumAggregate<V extends Number> extends Sum<V>
{
  /**
   * Emits sum and count if ports are connected
   */
  @Override
  public void endWindow()
  {
    //TBD

  }
}