/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.AbstractBaseNUniqueOperatorMap;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Orders tuples per key and emits top N unique tuples per key on end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input data port expects HashMap&lt;K,V&gt;<br>
 * <b>top</b>: Output data port, emits HashMap&lt;K, ArrayList&lt;V&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * N: Has to be >= 1<br>
 * <br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for TopNUnique&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 15 Million K,V pairs/s</b></td><td>Bottom N unique values per key per window</td><td>In-bound throughput and number of keys is the main determinant of performance.
 * Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); n=2</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for TopNUnique&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>bottom</i>(HashMap&lt;K,ArrayList&lt;V&gt;&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=2,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=1,a=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23,e=2}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{a=[{10=1},{5=2}]}<br>{b=[{20=1},{12=1}]}<br>{c=[{1000=1},{2=1}]}
 * <br>{d=[{55=1},{46=1}]}<br>{e=[{2=2}]}<br>{h=[{20=1}]}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class TopNUnique<K, V> extends AbstractBaseNUniqueOperatorMap<K, V>
{
  @OutputPortFieldAnnotation(name = "top")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<HashMap<V,Integer>>>> top = new DefaultOutputPort<HashMap<K, ArrayList<HashMap<V,Integer>>>>(this);

  /**
   * returns true
   * @return true
   */
  @Override
  public boolean isAscending()
  {
    return true;
  }

  /**
   * Emits tuple on port "top"
   * @param tuple
   */
  @Override
  public void emit(HashMap<K, ArrayList<HashMap<V,Integer>>> tuple)
  {
    top.emit(tuple);
  }
}
