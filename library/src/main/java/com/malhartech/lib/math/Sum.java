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
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Emits the sum, average, and count of values for each key at the end of window<p>
 * Is an end of window operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V extends Number&gt;<br>
 * <b>sum</b>: emits HashMap&lt;K,V&gt;<br>
 * <b>count</b>: emits HashMap&lt;K,Integer&gt;</b><br>
 * <b>average</b>: emits HashMap&lt;K,V&gt;</b><br><br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>18 Million K,V pairs/s</b></td><td>One K,V or K,Integer per key per window per port</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>sum</i>(HashMap&lt;K,V&gt;)</th><th><i>count</i>(HashMap&lt;K,Integer&gt;)</th><th><i>average</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23}</td><td></td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{a=36,b=37,c=1000,d=141,e=2}</td><td>{a=4,b=3,c=1,d=5,e=1}</td><td>{a=9,b=12,c=1000,d=28,e=2}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class Sum<K, V extends Number> extends BaseNumberKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * For each tuple (a HashMap of keys,val pairs)
     * Adds the values for each key.
     * Counts the number of occurences of each key
     * Computes the average
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        K key = e.getKey();
        if (sum.isConnected()) {
          MutableDouble val = sums.get(key);
          if (val == null) {
            val = new MutableDouble(e.getValue().doubleValue());
          }
          else {
            val.add(e.getValue().doubleValue());
          }
          sums.put(cloneKey(key), val);
        }
        if (count.isConnected() || average.isConnected()) {
          MutableInteger count = counts.get(key);
          if (count == null) {
            count = new MutableInteger(0);
            counts.put(cloneKey(key), count);
          }
          count.value++;
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "sum", optional=true)
  public final transient DefaultOutputPort<HashMap<K, V>> sum = new DefaultOutputPort<HashMap<K, V>>(this);
  @OutputPortFieldAnnotation(name = "average", optional=true)
  public final transient DefaultOutputPort<HashMap<K, V>> average = new DefaultOutputPort<HashMap<K, V>>(this);
  @OutputPortFieldAnnotation(name = "count", optional=true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>(this);
  HashMap<K, MutableDouble> sums = new HashMap<K, MutableDouble>();
  HashMap<K, MutableInteger> counts = new HashMap<K, MutableInteger>();

  /**
   * Clears the cache/hash
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    sums.clear();
    counts.clear();
  }

  /**
   * Emits on all ports that are connected. Data is precomputed during process on input port
   * endWindow just emits it for each key
   */
  @Override
  public void endWindow()
  {

    // Should allow users to send each key as a separate tuple to load balance
    // This is an aggregate node, so load balancing would most likely not be needed

    HashMap<K, V> stuples = null;
    if (sum.isConnected()) {
      stuples = new HashMap<K, V>();
    }

    HashMap<K, Integer> ctuples = null;
    if (count.isConnected()) {
      ctuples = new HashMap<K, Integer>();
    }

    HashMap<K, V> atuples = null;
    if (average.isConnected()) {
      atuples = new HashMap<K, V>();
    }

    if (sum.isConnected()) {
      for (Map.Entry<K, MutableDouble> e: sums.entrySet()) {
        K key = e.getKey();
        if (sum.isConnected()) {
          stuples.put(key, getValue(e.getValue().value));
        }
        if (count.isConnected()) {
          ctuples.put(key, new Integer(counts.get(e.getKey()).value));
        }
        if (average.isConnected()) {
          atuples.put(e.getKey(), getValue(e.getValue().value / counts.get(e.getKey()).value));
        }
      }
    }
    else if (count.isConnected()) { // sum is not connected, only counts is connected
      for (Map.Entry<K, MutableInteger> e: counts.entrySet()) {
        ctuples.put(e.getKey(), new Integer(e.getValue().value));
      }
    }

    if ((stuples != null) && !stuples.isEmpty()) {
      sum.emit(stuples);
    }
    if ((ctuples != null) && !ctuples.isEmpty()) {
      count.emit(ctuples);
    }
    if ((atuples != null) && !atuples.isEmpty()) {
      average.emit(atuples);
    }
  }
}
