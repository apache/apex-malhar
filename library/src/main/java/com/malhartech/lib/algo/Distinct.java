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
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Computes and emits distinct key,val pairs (i.e drops duplicates)<p>
 * This is a pass through operator<br>
 * <br>
 * This module is same as a "FirstOf" operation on any key,val pair. At end of window all data is flushed.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input data port expects HashMap&lt;K,V&gt;<br>
 * <b>distinct</b>: Output data port, emits HashMap&lt;K,V&gt;(1)<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None <br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Distinct&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 6 Million K,V pairs/s (at 4 million out-bound emits/s)</b></td><td>Emits first instance of a unique k,v pair</td><td>In-bound throughput and number of unique k,v pairs are the main determinant of performance.
 * Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Distinct&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>distinct</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td>{a=2}<br>{b=20}<br>{c=1000} </td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td>{a=-1}</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5}</td><td>{b=5}</td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=-5}</td><td>{a=5}<br>{b=-5}</td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=1000,b=-5}</td><td>{a=3}<br>{h=20}</td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=5}</td><td>{d=55}</td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td>{d=14}</td></tr>
 * <tr><td>Data (process())</td><td>{d=55,e=2}</td><td>{e=2}</td></tr>
 * <tr><td>Data (process())</td><td>{d=1,a=5,f=55}</td><td>{d=1}<br>{f=55}</td></tr>
 * <tr><td>Data (process())</td><td>{d=1,a=3,e=2}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 *
 *
 */
public class Distinct<K, V> extends BaseKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Process HashMap<K,V> tuple on input port data, and emits if match not found. Updates the cache
     * with new key,val pair
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        HashMap<V, Object> vals = mapkeyval.get(e.getKey());
        if ((vals == null) || !vals.containsKey(e.getValue())) {
          HashMap<K, V> otuple = new HashMap<K, V>(1);
          otuple.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
          distinct.emit(otuple);
          if (vals == null) {
            vals = new HashMap<V, Object>();
            mapkeyval.put(cloneKey(e.getKey()), vals);
          }
          vals.put(cloneValue(e.getValue()), null);
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "distinct")
  public final transient DefaultOutputPort<HashMap<K, V>> distinct = new DefaultOutputPort<HashMap<K, V>>(this);
  protected transient HashMap<K, HashMap<V, Object>> mapkeyval = new HashMap<K, HashMap<V, Object>>();

  /**
   * Clears the cache/hash
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    mapkeyval.clear();
  }
}
