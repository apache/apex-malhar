/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * Emits the average of values for each key at the end of window. <p>
 * This is an end window operator. This can not be partitioned. Partitioning this will yield incorrect result.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>average</b>: emits KeyValPair&lt;K,V extends Number&gt;</b><br><br>
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
 * <tr><td><b>13 million tuples/s</b></td><td>One tuple per key per port</td><td>Mainly dependant on in-bound throughput</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(KeyValPair&lt;K,V&gt;)</th><th><i>average</i>(KeyValPair&lt;K,V&gt;)</th></tr>
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
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>a=9<br>b=12<br>c=1000<br>d=28<br>e=2</td></tr>
 * </table>
 * <br>
 *
 * @param <K>
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public class AverageKeyVal<K> extends BaseNumberKeyValueOperator<K, Number>
{
  /**
   * Data input port.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, ? extends Number>> data = new DefaultInputPort<KeyValPair<K, ? extends Number>>(this)
  {
    /**
     * Adds the values for each key,
     * counts the number of occurrences of each key and
     * computes the average.
     */
    @Override
    public void process(KeyValPair<K, ? extends Number> tuple)
    {
      K key = tuple.getKey();
      if (!doprocessKey(key)) {
        return;
      }
      MutableDouble val = sums.get(key);
      if (val == null) {
        val = new MutableDouble(tuple.getValue().doubleValue());
      }
      else {
        val.add(tuple.getValue().doubleValue());
      }
      sums.put(cloneKey(key), val);

      MutableLong count = counts.get(key);
      if (count == null) {
        count = new MutableLong(0);
        counts.put(cloneKey(key), count);
      }
      count.increment();
    }
  };
  @OutputPortFieldAnnotation(name = "doubleAverage")
  public final transient DefaultOutputPort<KeyValPair<K, Double>> doubleAverage = new DefaultOutputPort<KeyValPair<K, Double>>(this);
  @OutputPortFieldAnnotation(name = "intAverage")
  public final transient DefaultOutputPort<KeyValPair<K, Integer>> intAverage = new DefaultOutputPort<KeyValPair<K, Integer>>(this);
  @OutputPortFieldAnnotation(name = "longAverage")
  public final transient DefaultOutputPort<KeyValPair<K, Long>> longAverage = new DefaultOutputPort<KeyValPair<K, Long>>(this);
  protected HashMap<K, MutableDouble> sums = new HashMap<K, MutableDouble>();
  protected HashMap<K, MutableLong> counts = new HashMap<K, MutableLong>();

  /**
   * Emits average for each key in end window. Data is precomputed during process on input port
   * Clears the internal data before return.
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, MutableDouble> e: sums.entrySet()) {
      K key = e.getKey();
      double d;
      doubleAverage.emit(new KeyValPair<K, Double>(key, d = e.getValue().doubleValue() / counts.get(key).doubleValue()));
      intAverage.emit(new KeyValPair<K, Integer>(key, (int)d));
      longAverage.emit(new KeyValPair<K, Long>(key, (long)d));
    }
    sums.clear();
    counts.clear();
  }
}
