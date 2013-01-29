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
 * <b>data1</b>: expects KeyValPair of K, V1 <br>
 * <b>data2</b>: expects KeyValPair of K, V2 <br>
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
 * <tr><td><b>&gt; 6400 tuples/s</td><td>All 2 in-bound tuples result in emit of 1 out-bound tuples</td><td>In-bound rate is the main determinant of performance</td></tr>
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
public abstract class KeyValueConsolidator<K, V1, V2> extends BaseOperator
{
  public ArrayList getObject(K k)
  {
    ArrayList val = result.get(k);
    if (val == null) {
      val = new ArrayList(2);
      val.add(0, null);
      val.add(1, null);
      result.put(k, val);
    }
    return val;
  }

  /**
   * Consolidate current tuple with existing values.
   *
   * @param tuple_key Key of current tuple.
   * @param tuple_val Value of current tuple.
   * @param list Current existing values.
   * @param port Input port number starting from 0.
   * @return combined tuple by consolidating current tuple value with new tuple value.
   */
  public abstract Object mergeKeyValue(K tuple_key, Object tuple_val, ArrayList list, int port);

  /**
   * Override this to construct new tuple and emit on your own defined output port.
   * @param obj arrayList of consolidated values identified by key.
   */
  public abstract void emitConsolidatedTuple(KeyValPair<K, ArrayList<Object>> obj);

  /**
   * First input port named "data1".
   */
  @InputPortFieldAnnotation(name = "data1")
  public final transient DefaultInputPort<KeyValPair<K, V1>> data1 = new DefaultInputPort<KeyValPair<K, V1>>(this)
  {
    int idx = 0;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V1> tuple)
    {
      K key = tuple.getKey();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, tuple.getValue(), list, idx));
    }
  };
  /**
   * Second input port named "data2".
   */
  @InputPortFieldAnnotation(name = "data2")
  public final transient DefaultInputPort<KeyValPair<K, V2>> data2 = new DefaultInputPort<KeyValPair<K, V2>>(this)
  {
    int idx = 1;

    /**
     * Merge with existing value
     */
    @Override
    public void process(KeyValPair<K, V2> tuple)
    {
      K key = tuple.getKey();
      ArrayList list = getObject(key);
      list.set(idx, mergeKeyValue(key, tuple.getValue(), list, idx));
    }
  };
  /**
   * Container for consolidated result
   */
  HashMap<K, ArrayList<Object>> result = new HashMap<K, ArrayList<Object>>();

  /**
   * Emits merged data
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, ArrayList<Object>> e: result.entrySet()) {
      emitConsolidatedTuple(new KeyValPair<K, ArrayList<Object>>(e.getKey(), e.getValue()));
    }
    result.clear();
  }
}
