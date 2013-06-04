/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseNumberKeyValueOperator;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * Emits the average value for each key at the end of window. <p>
 * This is an end window operator. This can not be partitioned. Partitioning this will yield incorrect result.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V extends Number&gt;<br>
 * <b>average</b>: emits HashMap&lt;K,V extends Number&gt;</b><br><br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>12 millions K,V pairs/s</b></td><td>One K,V per key per window per port</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower.</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>average</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{a=9,b=12,c=1000,d=28,e=2}</td></tr>
 * </table>
 * <br>
 *
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public class AverageMap<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  /**
   * Data input port.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>(this)
  {
    /**
     * For each tuple (a HashMap of key,val pairs)
     * adds the values for each key,
     * counts the number of occurrences of each key and
     * computes the average.
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        K key = e.getKey();
        if (!doprocessKey(key)) {
          continue;
        }

        MutableDouble val = sums.get(key);
        if (val == null) {
          val = new MutableDouble(e.getValue().doubleValue());
        }
        else {
          val.add(e.getValue().doubleValue());
        }
        sums.put(cloneKey(key), val);

        MutableLong count = counts.get(key);
        if (count == null) {
          count = new MutableLong(0);
          counts.put(cloneKey(key), count);
        }
        count.increment();
      }
    }
  };
  @OutputPortFieldAnnotation(name = "average")
  public final transient DefaultOutputPort<HashMap<K, V>> average = new DefaultOutputPort<HashMap<K, V>>(this)
  {
  };

  protected HashMap<K, MutableDouble> sums = new HashMap<K, MutableDouble>();
  protected HashMap<K, MutableLong> counts = new HashMap<K, MutableLong>();

  /**
   * Emits average for each key in end window. Data is precomputed during process on input port
   * Clears the internal data before return.
   */
  @Override
  public void endWindow()
  {
    HashMap<K, V> atuples = new HashMap<K, V>();
    for (Map.Entry<K, MutableDouble> e: sums.entrySet()) {
      K key = e.getKey();
      atuples.put(key, getValue(e.getValue().doubleValue() / counts.get(key).doubleValue()));
    }

    if (!atuples.isEmpty()) {
      average.emit(atuples);
    }
    sums.clear();
    counts.clear();
  }
}
