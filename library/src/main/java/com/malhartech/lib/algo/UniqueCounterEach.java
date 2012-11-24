/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseUniqueKeyCounter;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Counts the number of times a key exists in a window; one tuple is emitted per unique key pair<p>
 * This is an end of window operator. This operator is same as the combination of {@link com.malhartech.lib.algo.UniqueCounter} followed by {@link com.malhartech.lib.stream.HashMapToKey}<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>count</b>: emits HashMap&lt;K,Integer&gt;(1)<br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for UniqueCounter&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; processes 110 Million K,V pairs/s</b></td><td>Emits one tuple per window</td><td>In-bound throughput
 * and number of unique k are the main determinant of performance. Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys,
 * the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for UniqueCounter&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(K)</th><th><i>count</i>(HashMap&lt;K,Integer&gt;(1))</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>b</td><td></td></tr>
 * <tr><td>Data (process())</td><td>c</td><td></td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5ah</td><td></td></tr>
 * <tr><td>Data (process())</td><td>h</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5ah</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>c</td><td></td></tr>
 * <tr><td>Data (process())</td><td>c</td><td></td></tr>
 * <tr><td>Data (process())</td><td>b</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{a=6}<br>{b=2}<br>{c=3}<br>{5ah=2}<br>{h=1}<br>{4=1}</td></tr>
 * </table>
 * <br>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UniqueCounterEach<K> extends BaseUniqueKeyCounter<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {

    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>(this);

  @Override
  public void endWindow()
  {
    // emitting one key at a time helps in load balancing
    // If MutableInteger is supported, then there is no need to create a new hash
    // just emit(map) would suffice
    for (Map.Entry<K, MutableInteger> e: map.entrySet()) {
      HashMap<K, Integer> tuple = new HashMap<K, Integer>(1);
      tuple.put(e.getKey(), new Integer(e.getValue().value));
      count.emit(tuple);
    }
  }
}
