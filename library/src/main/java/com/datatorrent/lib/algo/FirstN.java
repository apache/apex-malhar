/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.AbstractBaseNOperatorMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableInt;

/**
 *
 * Emits first N tuples of a particular key.<p>
 * This module is a pass through module<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input data port expects HashMap&lt;K,V&gt;<br>
 * <b>bottom</b>: Output data port, emits HashMap&lt;K,V&gt;<br>
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
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for FirstN&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 5 Million K,V pairs/s</b></td><td>First N key,val pairs per key per window</td><td>In-bound throughput and N are the main determinant of performance.
 * Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); n=2</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for FirstN&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>first</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td>{a=2}<br>{b=20}<br>{c=1000}</td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td>{a=-1}</td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td>{b=5}</td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,d=14,h=20,c=2,b=-5}</td><td>{d=14}<br>{h=20}<br>{c=2}</td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td>{d=55}</td></tr>
 * <tr><td>Data (process())</td><td>{d=22,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2,b=5}</td><td>{e=2}</td></tr>
 * <tr><td>Data (process())</td><td>{d=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14,a=23,e=2,b=5}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * <br>
 */

public class FirstN<K,V> extends AbstractBaseNOperatorMap<K, V>
{
  @OutputPortFieldAnnotation(name="first")
  public final transient DefaultOutputPort<HashMap<K, V>> first = new DefaultOutputPort<HashMap<K, V>>();

  HashMap<K, MutableInt> keycount = new HashMap<K, MutableInt>();

  /**
   * Inserts tuples into the queue
   * @param tuple to insert in the queue
   */
  @Override
  public void processTuple(Map<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      MutableInt count = keycount.get(e.getKey());
      if (count == null) {
        count = new MutableInt(0);
        keycount.put(e.getKey(), count);
      }
      count.increment();
      if (count.intValue() <= getN()) {
        first.emit(cloneTuple(e.getKey(), e.getValue()));
      }
    }
  }

  /**
   * Clears the cache to start anew in a new window
   */
  @Override
  public void endWindow()
  {
    keycount.clear();
  }
}
