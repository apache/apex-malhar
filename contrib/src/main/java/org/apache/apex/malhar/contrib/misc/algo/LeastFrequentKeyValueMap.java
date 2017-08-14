/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.misc.algo;

import java.util.HashMap;

import org.apache.apex.malhar.lib.util.AbstractBaseFrequentKeyValueMap;

import com.datatorrent.api.DefaultOutputPort;

/**
 * This operator filters the incoming stream of key value pairs by finding the value or values (if there is a tie),
 * for each key, that occur the fewest number of times within each window.&nbsp;
 * Each key and its corresponding least values are emitted at the end of each window.
 * <p>
 * Occurrences of all values for each key is counted and at the end of window the least frequent values are emitted on output port least per key.
 * </p>
 * <p>
 * This module is an end of window module<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V&gt;<br>
 * <b>least</b>: Output port, emits HashMap&lt;K,HashMap&lt;V,Integer&gt;&gt;(1)<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None <br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for LeastFrequentKeyValueMap&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 30 Million K,V pairs/s</b></td><td>Emits only 1 tuple per window per key</td><td>In-bound throughput is the main determinant of performance.
 * The benchmark was done with immutable objects. If K or V are mutable the benchmark may be lower</td></tr>
 * </table><br>
 * </p>
 * <p>
 * <b>Function Table (K=String,V=Integer);</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for LeastFrequentKeyValueMap&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,V&gt;)</th><th><i>least</i>(HashMap&lt;K,HashMap&lt;Integer&gt;&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=5,c=110}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=55,c=2000,b=45}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=55,b=5,c=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{h=20,a=2,z=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=4,c=110}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=4,z=5}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{a={1=1,2=1},b={45=1},c={2000=1,22=1},d={2=1},h={20=1},z={5=2}</td></tr>
 * </table>
 * <br>
 * <br>
 * </p>
 *
 * @displayName Emit Least Frequent Keyval Pair
 * @category Rules and Alerts
 * @tags filter, key value, count
 * @deprecated
 * @since 0.3.2
 */
@Deprecated
public class LeastFrequentKeyValueMap<K, V> extends AbstractBaseFrequentKeyValueMap<K, V>
{
  /**
   * The output port on which the least frequent key value pairs are emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, HashMap<V, Integer>>> least = new DefaultOutputPort<HashMap<K, HashMap<V, Integer>>>();

  /**
   * returns val1 < val2
   * @param val1
   * @param val2
   * @return val1 < val2
   */
  @Override
  public boolean compareValue(int val1, int val2)
  {
    return (val1 < val2);
  }

  /**
   * Emits tuple on port "least"
   * @param tuple
   */
  @Override
  public void emitTuple(HashMap<K, HashMap<V, Integer>> tuple)
  {
    least.emit(tuple);
  }
}
