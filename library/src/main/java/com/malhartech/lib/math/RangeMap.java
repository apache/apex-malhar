/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.lib.util.BaseNumberKeyValueOperator;
import com.malhartech.lib.util.HighLow;
import com.malhartech.lib.util.UnifierHashMapRange;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 *
 * Emits the range for each key at the end of window. <p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>range</b>: emits HashMap&lt;K,HighLow&lt;V&gt;&gt; each key has two entries; .get(0) gives Max, .get(1) gives Min<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for RangeMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>10 Million K,V pairs/s</b></td><td>One K,ArrayList(2) pair per key per window</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for RangeMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,V&gt;)</th><th><i>range</i>(HashMap&lt;K,HighLow&lt;V&gt;&gt;)</th></tr>
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
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class RangeMap<K, V extends Number> extends BaseNumberKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>(this)
  {
    /**
     * Process each key and computes new high and low
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        K key = e.getKey();
        if (!doprocessKey(key) || (e.getValue() == null)) {
          continue;
        }
        V eval = e.getValue();
        V val = low.get(key);
        if ((val == null) || (val.doubleValue() > eval.doubleValue())) {
          low.put(cloneKey(key), eval);
        }

        val = high.get(key);
        if ((val == null) || (val.doubleValue() < eval.doubleValue())) {
          high.put(cloneKey(key), eval);
        }
      }
    }
  };


  @OutputPortFieldAnnotation(name = "range")
  public final transient DefaultOutputPort<HashMap<K, HighLow>> range = new DefaultOutputPort<HashMap<K, HighLow>>(this)
  {
    @Override
    public Unifier<HashMap<K, HighLow>> getUnifier()
    {
      return new UnifierHashMapRange<K>();
    }
  };

  protected transient HashMap<K,V> high = new HashMap<K,V>();
  protected transient HashMap<K,V> low = new HashMap<K,V>();

  /**
   * Emits range for each key. If no data is received, no emit is done
   * Clears the internal data before return
   */
  @Override
  public void endWindow()
  {
    HashMap<K, HighLow> tuples = new HashMap<K, HighLow>(1);
    for (Map.Entry<K,V> e: high.entrySet()) {
      tuples.put(e.getKey(), new HighLow(e.getValue(), low.get(e.getKey())));
    }
    if (!tuples.isEmpty()) {
      range.emit(tuples);
    }
    clearCache();
  }

  public void clearCache()
  {
    high.clear();
    low.clear();
  }
}
