/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseNumberKeyValueOperator;
import com.malhartech.lib.util.MutableDouble;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Emits at end of window minimum of all values sub-classed from Number for each key<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>min</b>: emits HashMap&lt;K,V&gt;, one entry per key<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for MinMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>20 Million K,V pairs/s</b></td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for MinMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,V&gt;)</th><th><i>min</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=-23}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{a=-23,b=5,c=1000,d=4,e=2}</td></tr>
 * </table>
 * <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class MinMap<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * For each key, updates the hash if the new value is a new min
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        if (!doprocessKey(e.getKey()) || (e.getValue() == null)) {
          continue;
        }
        MutableDouble val = low.get(e.getKey());
        if (val == null) {
          val = new MutableDouble(e.getValue().doubleValue());
          low.put(cloneKey(e.getKey()), val);
        }
        if (val.value > e.getValue().doubleValue()) {
          val.value = e.getValue().doubleValue();
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "min")
  public final transient DefaultOutputPort<HashMap<K, V>> min = new DefaultOutputPort<HashMap<K, V>>(this);
  protected transient HashMap<K, MutableDouble> low = new HashMap<K, MutableDouble>();

  /**
   * Node only works in windowed mode. Emits all key,minval pairs
   * Override getValue() if you have your own class extended from Number
   * Clears internal data
   */
  @Override
  public void endWindow()
  {
    if (!low.isEmpty()) {
      HashMap<K, V> tuple = new HashMap<K, V>(low.size());
      for (Map.Entry<K, MutableDouble> e: low.entrySet()) {
        tuple.put(e.getKey(), getValue(e.getValue().value));
      }
      min.emit(tuple);
    }
    low.clear();
  }
}
