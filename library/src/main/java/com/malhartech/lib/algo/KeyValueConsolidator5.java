/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Merges two streams of Key Value pair and emits the tuples to the output port at the end of window.<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data0</b>: expects KeyValPair of K, V1<br>
 * <b>data1</b>: expects KeyValPair of K, V2<br>
 * <b>out</b>: No output port in this abstract class. The concrete class should define the output port and emit on it. K<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for StreamMerger&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 500 Million tuples/s</td><td>Each in-bound tuple results in emit of 1 out-bound tuples</td><td>In-bound rate is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for StreamMerger&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th colspan=2>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data1</i>(K)</th><th><i>data2</i>(K)</th><th><i>out</i>(K)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td>a</td></tr>
 * <tr><td>Data (process())</td><td></td><td>b</td><td>b</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public abstract class KeyValueConsolidator5<K, V1 extends Object, V2 extends Object, V3 extends Object, V4 extends Object, V5 extends Object> extends KeyValueConsolidator<K, V1, V2>
{
  @Override
  public ArrayList getObject(K k)
  {
    ArrayList val = result.get(k);
    if (val == null) {
      val = new ArrayList(5);
      val.add(0, null);
      val.add(1, null);
      val.add(2, null);
      val.add(3, null);
      val.add(4, null);
      result.put(k, val);
    }
    return val;
  }
  /**
   * Third input port named "data3".
   */
  @InputPortFieldAnnotation(name = "data3")
  public final transient DefaultInputPort<KeyValPair<K, V3>> data3 = new DefaultInputPort<KeyValPair<K, V3>>(this)
  {
    int idx = 2;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V3> tuple)
    {
      K key = tuple.getKey();
      V3 value = tuple.getValue();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, value, list, idx));
    }
  };
  /**
   * Fourth input port named "data4".
   */
  @InputPortFieldAnnotation(name = "data4")
  public final transient DefaultInputPort<KeyValPair<K, V4>> data4 = new DefaultInputPort<KeyValPair<K, V4>>(this)
  {
    int idx = 3;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V4> tuple)
    {
      K key = tuple.getKey();
      V4 value = tuple.getValue();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, value, list, idx));
    }
  };
  /**
   * Fourth input port named "data5".
   */
  @InputPortFieldAnnotation(name = "data5")
  public final transient DefaultInputPort<KeyValPair<K, V5>> data5 = new DefaultInputPort<KeyValPair<K, V5>>(this)
  {
    int idx = 4;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V5> tuple)
    {
      K key = tuple.getKey();
      V5 value = tuple.getValue();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, value, list, idx));
    }
  };

}
