/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableDouble;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Emits the range for each key at the end of window<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b> expects HashMap&lt;K,V extends Number&gt;<br>
 * <b>range</b> emits HashMap&lt;K,ArrayList&lt;V&gt;&gt; each key has two entries; .get(0) gives Max, .get(1) gives Min<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Range&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>10 Million K,V pairs/s</td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Range&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>range</i>(HashMap&lt;K,ArrayList&li;V&gt;&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td>/tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{a=[23,-1],b=[20,5],c=[1000,1000],d=[55,4],e=[2,2]</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class Range<K, V extends Number> extends BaseNumberKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Process each key and computes new high and low
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        K key = e.getKey();
        if (e.getValue() == null) {
          continue;
        }
        double eval = e.getValue().doubleValue();
        MutableDouble val = low.get(key);
        if (val == null) {
          low.put(cloneKey(key), new MutableDouble(eval));
        }
        else if (val.value > eval) {
          val.value = eval;
        }

        val = high.get(key);
        if (val == null) {
          high.put(cloneKey(key), new MutableDouble(eval));
        }
        else if (val.value < eval) {
          val.value = eval;
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "range")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> range = new DefaultOutputPort<HashMap<K, ArrayList<V>>>(this);
  HashMap<K,MutableDouble> high = new HashMap<K,MutableDouble>();
  HashMap<K,MutableDouble> low = new HashMap<K,MutableDouble>();

  /**
   * Clears the cache/hash
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    high.clear();
    low.clear();
  }

  /**
   * Emits range for each key. If no data is received, no emit is done
   */
  @Override
  public void endWindow()
  {
    HashMap<K, ArrayList<V>> tuples = new HashMap<K, ArrayList<V>>(1);
    for (Map.Entry<K,MutableDouble> e: high.entrySet()) {
      ArrayList<V> alist = new ArrayList<V>();
      alist.add(getValue(e.getValue().value));
      alist.add(getValue(low.get(e.getKey()).value)); // cannot be null
      tuples.put(e.getKey(), alist);
    }
    if (!tuples.isEmpty()) {
      range.emit(tuples);
    }
  }
}
