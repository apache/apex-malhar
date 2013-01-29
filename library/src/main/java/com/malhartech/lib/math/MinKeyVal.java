/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.StreamCodec;
import com.malhartech.lib.util.BaseNumberKeyValueOperator;
import com.malhartech.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Emits at end of window minimum of all values sub-classed from Number for each key in KeyValPair. <p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>min</b>: emits KeyValPair&lt;K,V extends Number&gt;, one entry per key<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for MaxMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>35 Million K,V pairs/s</b></td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower.</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for MaxMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(KeyValPair&lt;K,V&gt;)</th><th><i>min</i>(KeyValPair&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a=2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>b=20</td><td></td></tr>
 * <tr><td>Data (process())</td><td>c=1000</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a=1</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a=10</td><td></td></tr>
 * <tr><td>Data (process())</td><td>b=5</td><td></td></tr>
 * <tr><td>Data (process())</td><td>d=55</td><td></td></tr>
 * <tr><td>Data (process())</td><td>b=12</td><td></td></tr>
 * <tr><td>Data (process())</td><td>d=22</td><td></td></tr>
 * <tr><td>Data (process())</td><td>d=14</td><td></td></tr>
 * <tr><td>Data (process())</td><td>d=46</td><td></td></tr>
 * <tr><td>Data (process())</td><td>e=2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>d=4</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a=23</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>a=1<br>b=5<br>c=1000<br>d=4<br>e=2</td></tr>
 * </table>
 * <br>
 *
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public class MinKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * For each key, updates the hash if the new value is a new min.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      V tval = tuple.getValue();
      if (!doprocessKey(key) || (tval == null)) {
        return;
      }
      V val = mins.get(key);
      if (val == null) {
        mins.put(cloneKey(key), tval);
      }
      else if (val.doubleValue() > tval.doubleValue()) {
        mins.put(key, tval);
      }
    }

    /**
     * Set StreamCodec used for partitioning.
     */
    @Override
    public Class<? extends StreamCodec<KeyValPair<K, V>>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }
  };
  @OutputPortFieldAnnotation(name = "max")
  public final transient DefaultOutputPort<KeyValPair<K, V>> min = new DefaultOutputPort<KeyValPair<K, V>>(this);
  protected transient HashMap<K, V> mins = new HashMap<K, V>();

  /**
   * Emits all key,min value pairs.
   * Clears internal data. Node only works in windowed mode.
   */
  @Override
  public void endWindow()
  {
    if (!mins.isEmpty()) {
      for (Map.Entry<K, V> e: mins.entrySet()) {
        min.emit(new KeyValPair(e.getKey(), e.getValue()));
      }
      mins.clear();
    }
  }
}
