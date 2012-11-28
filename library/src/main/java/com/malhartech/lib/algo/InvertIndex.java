/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyValueOperator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Inverts the index and sends out the tuple on output port "index" at the end of the window<p>
 * This is an end of window operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects &lt;K,V&gt;<br>
 * <b>index</b>: emits &lt;V,ArrayList&lt;K&gt;&gt;<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks are</b>: None<br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for InvertIndex&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 7 Million K,V pairs/s</b></td><td>All tuples are added to invert index per window, and the index is emitted at the end of window</td>
 * <td>In-bound throughput and value distribution are the main determinant of performance. Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); key=a; value=3; cmp=eq</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for InvertIndex&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>index</i>(HashMap&lt;V,ArrayList&lt;K&gt;&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=str,b=str}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=str1,b=str1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{c=str2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{c=str1}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{str1=[b, a, c]}<br>{str=[b, a]}<br>{str2=[c]}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class InvertIndex<K, V> extends BaseKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Reverse indexes a HashMap<K, ArrayList<V>> tuple
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        if (e.getValue() == null) { // error tuple?
          continue;
        }
        insert(e.getValue(), cloneKey(e.getKey()));
      }
    }
  };
  @OutputPortFieldAnnotation(name = "index")
  public final transient DefaultOutputPort<HashMap<V, ArrayList<K>>> index = new DefaultOutputPort<HashMap<V, ArrayList<K>>>(this);
  HashMap<V, ArrayList<K>> map = new HashMap<V, ArrayList<K>>();

  /**
   *
   * Returns the ArrayList stored for a key
   *
   * @param key
   * @return ArrayList
   */
  void insert(V val, K key)
  {
    ArrayList<K> list = map.get(val);
    if (list == null) {
      list = new ArrayList<K>(4);
      map.put(cloneValue(val), list);
    }
    list.add(key);
  }

  /**
   * Clears cache/hash
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    map.clear();
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<V, ArrayList<K>> e: map.entrySet()) {
      HashMap<V, ArrayList<K>> tuple = new HashMap<V, ArrayList<K>>(1);
      tuple.put(e.getKey(), e.getValue());
      index.emit(tuple);
    }
  }
}
