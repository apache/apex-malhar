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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Emits the range for each key at the end of window. <p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>range</b>: emits KeyValPair&lt;K,ArrayList&lt;V&gt;&gt; each key has two entries; .get(0) gives Max, .get(1) gives Min<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Range&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>16 Million K,V pairs/s</b></td><td>One K,ArrayList(2) pair per key per window</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Range&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>range</i>(HashMap&lt;K,ArrayList&lt;V&gt;&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
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
 *
 * @author Locknath Shil<br>
 * <br>
 */
public class RangeKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * Process each key and computes new high and low.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      if (!doprocessKey(key) || (tuple.getValue() == null)) {
        return;
      }
      double eval = tuple.getValue().doubleValue();
      MutableDouble val = low.get(key);
      if (val == null) {
        low.put(cloneKey(key), new MutableDouble(eval));
      }
      else if (val.value > eval) { // update low
        val.value = eval;
      }

      val = high.get(key);
      if (val == null) {
        high.put(cloneKey(key), new MutableDouble(eval));
      }
      else if (val.value < eval) { // updagte high
        val.value = eval;
      }
      processMetaData(tuple);
    }

    @Override
    public Class<? extends StreamCodec<KeyValPair<K, V>>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }
  };

  /**
   * Output port to send out the high low range.
   */
  @OutputPortFieldAnnotation(name = "range")
  public final transient DefaultOutputPort<KeyValPair<K, ArrayList<V>>> range = new DefaultOutputPort<KeyValPair<K, ArrayList<V>>>(this);
  protected transient HashMap<K, MutableDouble> high = new HashMap<K, MutableDouble>();
  protected transient HashMap<K, MutableDouble> low = new HashMap<K, MutableDouble>();

  /**
   * If you have extended from KeyValPair class and want to do some processing per tuple
   * override this call back.
   */
  public void processMetaData(KeyValPair<K, V> tuple)
  {
  }

  /**
   * Creates a KeyValPair tuple, override if you want to extend KeyValPair.
   *
   * @param k
   * @param v
   * @return new key value pair.
   */
  public KeyValPair<K, ArrayList<V>> cloneRangeTuple(K k, ArrayList<V> v)
  {
    return new KeyValPair(k, v);
  }

  /**
   * Emits range for each key. If no data is received, no emit is done
   * Clears the internal data before return
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, MutableDouble> e: high.entrySet()) {
      ArrayList<V> alist = new ArrayList<V>();
      alist.add(getValue(e.getValue().value));
      alist.add(getValue(low.get(e.getKey()).value)); // cannot be null

      range.emit(cloneRangeTuple(e.getKey(), alist));
    }

    high.clear();
    low.clear();
  }
}
