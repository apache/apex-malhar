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
import com.malhartech.lib.util.MutableDouble;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Emits the average of values for each key at the end of window. <p>
 * This is an end window operator.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>average</b>: emits KeyValPair&lt;K,V&gt;</b><br><br>
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
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public class AverageKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  /**
   * Data input port.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * Adds the values for each key,
     * counts the number of occurrences of each key and
     * computes the average.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
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

      MutableInteger count = counts.get(key);
      if (count == null) {
        count = new MutableInteger(0);
        counts.put(cloneKey(key), count);
      }
      count.value++;

      processMetaData(tuple);
    }

    @Override
    public Class<? extends StreamCodec<KeyValPair<K, V>>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }
  };
  @OutputPortFieldAnnotation(name = "average")
  public final transient DefaultOutputPort<KeyValPair<K, V>> average = new DefaultOutputPort<KeyValPair<K, V>>(this);
  protected transient HashMap<K, MutableDouble> sums = new HashMap<K, MutableDouble>();
  protected transient HashMap<K, MutableInteger> counts = new HashMap<K, MutableInteger>();

  /*
   * If you have extended from KeyValPair class and want to do some processing per tuple
   * overrise this call back.
   */
  public void processMetaData(KeyValPair<K, V> tuple)
  {
  }

  /**
   * Creates a KeyValPair tuple, override if you want to extend KeyValPair
   *
   * @param k
   * @param v
   * @return new key value pair.
   */
  public KeyValPair<K, V> cloneAverageTuple(K k, V v)
  {
    return new KeyValPair(k, v);
  }

  /**
   * Emits average for each key in end window. Data is precomputed during process on input port
   * Clears the internal data before return.
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, MutableDouble> e: sums.entrySet()) {
      K key = e.getKey();
      average.emit(cloneAverageTuple(key, getValue(e.getValue().value / counts.get(key).value)));
    }

    sums.clear();
    counts.clear();
  }
}
