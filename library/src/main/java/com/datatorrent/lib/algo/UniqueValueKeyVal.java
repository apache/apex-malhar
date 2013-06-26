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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyOperator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Count unique occurrences of vals for every key within a window, and emits Key,Integer pairs tuple.<p>
 * This is an end of window operator. It uses sticky key partition and default unifier<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V&gt;<br>
 * <b>count</b>: emits KeyValPair&lt;K,Integer&gt;<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for UniqueValueKeyVal&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; processes 3 Million K,V pairs/s</b></td><td>Emits one tuple per key per window</td><td>In-bound throughput
 * and number of unique vals per key are the main determinant of performance. Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys,
 * the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String)</b>: The order of the K,V pairs in the tuple is undeterminable
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for UniqueValueKeyVal&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>KeyValPair(K,V)</th><th><i>count</i>(KeyValPair(K,Integer)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=2,c=3}</td></tr>
 * <tr><td>Data (process())</td><td>{b=5}</td><td></td>/tr>
 * <tr><td>Data (process())</td><td>{c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{5ah=10}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=4,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1,c=3}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{5ah=10}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=4,b=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{c=3,b=2}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{{a=2},{5ah=1},{b=2},{c=3}</td></tr>
 * </table>
 * <br>
 */
public class UniqueValueKeyVal<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K,? extends Object>> data = new DefaultInputPort<KeyValPair<K,? extends Object>>()
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(KeyValPair<K,? extends Object> tuple)
    {
      HashSet<Object> vals = map.get(tuple.getKey());
      if (vals == null) {
        vals = new HashSet<Object>();
        map.put(cloneKey(tuple.getKey()), vals);
      }
      vals.add(tuple.getValue());
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<KeyValPair<K,Integer>> count = new DefaultOutputPort<KeyValPair<K,Integer>>();

  /**
   * Bucket counting mechanism.
   */
  protected HashMap<K, HashSet<Object>> map = new HashMap<K, HashSet<Object>>();


  /**
   * Emits key,Integer pairs
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K,HashSet<Object>> e: map.entrySet()) {
      count.emit(new KeyValPair(e.getKey(), e.getValue().size()));
    }
    clearCache();
  }

  public void clearCache()
  {
    map.clear();
  }
}
