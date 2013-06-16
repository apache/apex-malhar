/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

import java.util.Collection;

/**
 *
 * Adds incoming tuple to the state. This is a stateful operator that never flushes its state; i.e. the addition would go on forever. The result of each
 * addition is emitted on the four ports, namely \"doubleResult\", \"floatResult\", \"integerResult\", \"longResult\". Input tuple
 * object has to be an implementation of the interface Collection&lt;T&gt;. Tuples are emitted on the output ports only if they are
 * connected. This is done to avoid the cost of calling the functions when some ports are not connected.<p>
 * This is a stateful pass through operator<br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Collection&lt;T extends Number&lt;<br>
 * <b>doubleResult</b>: emits Double<br>
 * <b>floatResult</b>: emits Float<br>
 * <b>integerResult</b>: emits Integer<br>
 * <b>longResult</b>: emits Long<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Sigma&lt;T extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>20 million tuples/sec</b></td><td>emits one tuple per connected port per incoming tuple</td><td>In-bound rate and i/o are the main determinants</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sigma&lt;T extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=4>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i></th><th><i>doubleResult</i></th><th><i>floatResult</i></th><th><i>integerResult</i></th><th><i>longResult</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td>2.0</td><td>2.0</td><td>2</td><td>2</td></tr>
 * <tr><td>Data (process())</td><td>3</td><td>5.0</td><td>5.0</td><td>5</td><td>5</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>10</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>12</td><td>14.0</td><td>14.0</td><td>14</td><td>14</td></tr>
 * <tr><td>Data (process())</td><td>1</td><td>15.0</td><td>15.0</td><td>15</td><td>15</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>10</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @param <T>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Sigma<T extends Number> extends AggregateAbstractCalculus<T>
{
  @Override
  public long aggregateLongs(Collection<T> collection)
  {
    long l = 0;

    for (Number n: collection) {
      l += n.longValue();
    }

    return l;
  }

  @Override
  public double aggregateDoubles(Collection<T> collection)
  {
    double d = 0;

    for (Number n: collection) {
      d += n.doubleValue();
    }

    return d;
  }

}
