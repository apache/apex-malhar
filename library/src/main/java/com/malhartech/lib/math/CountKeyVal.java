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
import com.malhartech.lib.util.BaseKeyValueOperator;
import com.malhartech.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 *
 * Emits the sum, and count of values for each key at the end of window. <p>
 * This is an end window operator. Default unifier works as this operator follows sticky partition<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V&gt;<br>
 * <b>count</b>: emits KeyValPair&lt;K,Integer&gt;</b><br>
 * <br>
 * <b>Properties</b>: None<br>
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
 * <tr><th><i>data</i>(KeyValPair&lt;K,V&gt;)</th><th><i>count</i>(KeyValPair&lt;K,Integer&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=20}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=23}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td>
 * <td>{a=4}<br>{b=3}<br>{c=1}<br>{d=5}<br>{e=1}</td></tr>
 * </table>
 * <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class CountKeyVal<K, V> extends BaseKeyValueOperator<K, V>
{
  /**
   * Input port to receive data.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * For each tuple (a key value pair):
     * Adds the values for each key,
     * Counts the number of occurrence of each key, and
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      MutableInt count = counts.get(key);
      if (count == null) {
        count = new MutableInt(0);
        counts.put(cloneKey(key), count);
      }
      count.increment();
    }

    @Override
    public Class<? extends StreamCodec<KeyValPair<K, V>>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }
  };

  @OutputPortFieldAnnotation(name = "count", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Integer>> count = new DefaultOutputPort<KeyValPair<K, Integer>>(this);

  protected transient HashMap<K, MutableInt> counts = new HashMap<K, MutableInt>();

  /**
   * Emits on all ports that are connected. Data is precomputed during process on input port
   * and endWindow just emits it for each key.
   * Clears the internal data if resetAtEndWindow is true.
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, MutableInt> e: counts.entrySet()) {
      count.emit(new KeyValPair(e.getKey(), new Integer(e.getValue().intValue())));
    }
    clearCache();
  }

  public void clearCache()
  {
    counts.clear();
  }
}
