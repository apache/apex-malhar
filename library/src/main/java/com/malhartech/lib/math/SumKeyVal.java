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
import com.malhartech.lib.util.KeyValPair.Codec;
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
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>sum</b>: emits KeyValPair&lt;K,V&gt;<br>
 * <b>count</b>: emits KeyValPair&lt;K,Integer&gt;</b><br>
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
 * <tr><td><b>20 million tuples/s</b></td><td>One tuple per key per port</td><td>Mainly dependant on in-bound throughput</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(KeyValPair&lt;K,V&gt;)</th><th><i>sum</i>(KeyValPair&lt;K,V&gt;)</th><th><i>count</i>(KeyValPair&lt;K,Integer&gt;)</th><th><i>average</i>(KeyValPair&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=20}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{c=1000}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=5}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=12}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{e=2}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=23}</td><td></td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td>
 * <td>{a=36}<br>{b=37}<br>{c=1000}<br>{d=141}<br>{e=2}</td>
 * <td>{a=4}<br>{b=3}<br>{c=1}<br>{d=5}<br>{e=1}</td>
 * <td>{a=9}<br>{b=12}<br>{c=1000}<br>{d=28}<br>{e=2}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class SumKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * For each tuple (a HashMap of keys,val pairs)
     * Adds the values for each key.
     * Counts the number of occurences of each key
     * Computes the average
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      if (!doprocessKey(key)) {
        return;
      }
      if (sum.isConnected()) {
        MutableDouble val = sums.get(key);
        if (val == null) {
          val = new MutableDouble(tuple.getValue().doubleValue());
        }
        else {
          val.add(tuple.getValue().doubleValue());
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
      processMetaData(tuple);
    }

    @Override
    public Class<? extends StreamCodec<KeyValPair<K, V>>> getStreamCodec()
    {
      Class c = Codec.class;
      return (Class<? extends StreamCodec<KeyValPair<K, V>>>) c;
    }
  };
  @OutputPortFieldAnnotation(name = "sum", optional=true)
  public final transient DefaultOutputPort<KeyValPair<K, V>> sum = new DefaultOutputPort<KeyValPair<K, V>>(this);
  @OutputPortFieldAnnotation(name = "average", optional=true)
  public final transient DefaultOutputPort<KeyValPair<K, V>> average = new DefaultOutputPort<KeyValPair<K, V>>(this);
  @OutputPortFieldAnnotation(name = "count", optional=true)
  public final transient DefaultOutputPort<KeyValPair<K, Integer>> count = new DefaultOutputPort<KeyValPair<K, Integer>>(this);

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
   * @param k
   * @param v
   * @return
   */

  public KeyValPair<K, V> cloneSumTuple(K k, V v)
  {
    return new KeyValPair(k, v);
  }

  /**
   * Creates a KeyValPair tuple, override if you want to extend KeyValPair
   * @param k
   * @param v
   * @return
   */
  public KeyValPair<K, V> cloneAverageTuple(K k, V v)
  {
    return new KeyValPair(k, v);
  }

  /**
   * Creates a KeyValPair tuple, override if you want to extend KeyValPair
   * @param k
   * @param v
   * @return
   */
  public KeyValPair<K, Integer> cloneCountTuple(K k, Integer v)
  {
    return new KeyValPair(k, v);
  }

  /**
   * Emits on all ports that are connected. Data is precomputed during process on input port
   * endWindow just emits it for each key
   * Clears the internal data before return
   */
  @Override
  public void endWindow()
  {
    boolean dosum = sum.isConnected();
    boolean doaverage = average.isConnected();
    boolean docount = count.isConnected();

    if (dosum) {
      for (Map.Entry<K, MutableDouble> e: sums.entrySet()) {
        K key = e.getKey();
        sum.emit(cloneSumTuple(key, getValue(e.getValue().value)));
        if (docount) {
          count.emit(cloneCountTuple(key, new Integer(counts.get(e.getKey()).value)));
        }
        if (doaverage) {
          average.emit(cloneAverageTuple(key, getValue(e.getValue().value / counts.get(e.getKey()).value)));
        }
      }
    }
    else if (count.isConnected()) { // sum is not connected, only counts is connected
      for (Map.Entry<K, MutableInteger> e: counts.entrySet()) {
        count.emit(new KeyValPair(e.getKey(), new Integer(e.getValue().value)));
      }
    }
    sums.clear();
    counts.clear();
  }
}
