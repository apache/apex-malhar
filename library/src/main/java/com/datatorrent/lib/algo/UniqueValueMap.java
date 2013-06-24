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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyOperator;
import com.datatorrent.lib.util.BaseKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.UnifierHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableInt;

/**
 * Count unique occurrences of vals for every key within a window, and emits Key,Integer pairs tuple.<p>
 * This is an end of window operator. It uses sticky key partition and default unifier<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V&gt;<br>
 * <b>count</b>: emits HashMap&lt;K,Integer&gt;<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for UniqueValueMap&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; processes 3 Million K,V pairs/s</b></td><td>Emits one tuple per key per window</td><td>In-bound throughput
 * and number of unique vals per key are the main determinant of performance. Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys,
 * the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String)</b>: The order of the K,V pairs in the tuple is undeterminable
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for UniqueValueMap&lt;K,V&gt; operator template">
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
public class UniqueValueMap<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, ? extends Object>> data = new DefaultInputPort<Map<K, ? extends Object>>()
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(Map<K, ? extends Object> tuple)
    {
      for (Map.Entry<K, ? extends Object> e: tuple.entrySet()) {
        HashSet<Object> vals = map.get(e.getKey());
        if (vals == null) {
          vals = new HashSet<Object>();
          map.put(cloneKey(e.getKey()), vals);
        }
        vals.add(e.getValue());
      }
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      return new UnifierHashMap<K, Integer>();
    }
  };

  /**
   * Bucket counting mechanism.
   */
  protected HashMap<K, HashSet<Object>> map = new HashMap<K, HashSet<Object>>();


  /**
   * Emits one HashMap as tuple
   */
  @Override
  public void endWindow()
  {
    if (!map.isEmpty()) {
      HashMap<K, Integer> tuple = new HashMap<K, Integer>(map.size());
      for (Map.Entry<K, HashSet<Object>> e: map.entrySet()) {
        tuple.put(e.getKey(), e.getValue().size());
      }
      count.emit(tuple);
      clearCache();
    }
  }

  public void clearCache()
  {
    map.clear();
  }
}
