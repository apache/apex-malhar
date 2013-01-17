/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.lib.util.KeyValPair;
import java.util.ArrayList;

/**
 * Merges upto 10 streams of Key Value pair and emits the tuples to the output port at the end of window.<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data1</b>: expects KeyValPair of K, V1<br>
 * <b>data2</b>: expects KeyValPair of K, V2<br>
 * <b>data3</b>: expects KeyValPair of K, V3<br>
 * <b>data4</b>: expects KeyValPair of K, V4<br>
 * <b>data5</b>: expects KeyValPair of K, V5<br>
 * <b>data6</b>: expects KeyValPair of K, V6<br>
 * <b>data7</b>: expects KeyValPair of K, V7<br>
 * <b>data8</b>: expects KeyValPair of K, V8<br>
 * <b>data9</b>: expects KeyValPair of K, V9<br>
 * <b>data10</b>: expects KeyValPair of K, V10<br>
 * <b>out</b>: No output port in this abstract class. The concrete class should define the output port and emit on it. <br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for StreamMerger&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 19 thousand tuples/s</td><td>All 10 in-bound tuples result in emit of 1 out-bound tuples</td><td>In-bound rate is the main determinant of performance</td></tr>
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
public abstract class KeyValueConsolidator10<K, V1 extends Object, V2 extends Object, V3 extends Object, V4 extends Object, V5 extends Object,
        V6 extends Object, V7 extends Object, V8 extends Object, V9 extends Object, V10 extends Object> extends KeyValueConsolidator5<K, V1, V2, V3, V4, V5>
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
      val.add(5, null);
      val.add(6, null);
      val.add(7, null);
      val.add(8, null);
      val.add(9, null);
      result.put(k, val);
    }
    return val;
  }
  /**
   * Sixth input port named "data6".
   */
  @InputPortFieldAnnotation(name = "data6", optional = true)
  public final transient DefaultInputPort<KeyValPair<K, V6>> data6 = new DefaultInputPort<KeyValPair<K, V6>>(this)
  {
    int idx = 5;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V6> tuple)
    {
      K key = tuple.getKey();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, tuple.getValue(), list, idx));
    }
  };
  /**
   * Seventh input port named "data7".
   */
  @InputPortFieldAnnotation(name = "data7", optional = true)
  public final transient DefaultInputPort<KeyValPair<K, V7>> data7 = new DefaultInputPort<KeyValPair<K, V7>>(this)
  {
    int idx = 6;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V7> tuple)
    {
      K key = tuple.getKey();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, tuple.getValue(), list, idx));
    }
  };
  /**
   * Eighth input port named "data8".
   */
  @InputPortFieldAnnotation(name = "data8", optional = true)
  public final transient DefaultInputPort<KeyValPair<K, V8>> data8 = new DefaultInputPort<KeyValPair<K, V8>>(this)
  {
    int idx = 7;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V8> tuple)
    {
      K key = tuple.getKey();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, tuple.getValue(), list, idx));
    }
  };
  /**
   * Ninth input port named "data9".
   */
  @InputPortFieldAnnotation(name = "data9", optional = true)
  public final transient DefaultInputPort<KeyValPair<K, V9>> data9 = new DefaultInputPort<KeyValPair<K, V9>>(this)
  {
    int idx = 8;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V9> tuple)
    {
      K key = tuple.getKey();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, tuple.getValue(), list, idx));
    }
  };
  /**
   * Tenth input port named "data10".
   */
  @InputPortFieldAnnotation(name = "data10", optional = true)
  public final transient DefaultInputPort<KeyValPair<K, V10>> data10 = new DefaultInputPort<KeyValPair<K, V10>>(this)
  {
    int idx = 9;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V10> tuple)
    {
      K key = tuple.getKey();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, tuple.getValue(), list, idx));
    }
  };
}
