/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyOperator;
import java.util.ArrayList;

/**
 * Takes in an ArrayList and emits each item in the array; mainly used for breaking up a ArrayList tuple into Objects<p>
 * <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects ArrayList&lt;K>&gt;br>
 * <b>item</b>: emits K<br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for ArrayListToItem&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 160 Million tuples/s</td><td>Each in-bound tuple results in emit of N out-bound tuples, where N is average size of ArrayList</td><td>In-bound rate and average ArrayList size is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for ArrayListToItem&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(&lt;K&gt;)</th><th><i>sum</i>(&lt;V&gt;)</th><th><i>count</i>(Integer)</th><th><i>average</i>(&lt;V&gt;)</th></tr>
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
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class ArrayListToItem<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>(this)
  {
    /**
     * Emitting one item at a time
     */
    @Override
    public void process(ArrayList<K> tuple)
    {
      for (K k: tuple) {
        item.emit(cloneKey(k));
      }
    }
  };
  @OutputPortFieldAnnotation(name = "item")
  public final transient DefaultOutputPort<K> item = new DefaultOutputPort<K>(this);
}
