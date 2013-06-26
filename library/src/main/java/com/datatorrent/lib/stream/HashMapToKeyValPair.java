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
package com.datatorrent.lib.stream;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * Takes a HashMap and emits its keys, keyvals, vals; used for breaking up a HashMap tuple into objects (keys, vals, or key/val pairs)<p>
 * This is a pass through operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>key</b>: emits K<br>
 * <b>keyval</b>: emits Entry&lt;K,V&gt;<br>
 * <b>val</b>: emits V<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for HashMapToKeyValPair&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 25 Million tuples/s</td><td>Each in-bound tuple results in emits 3*N out-bound tuples, where N is average size of HashMap</td><td>In-bound rate and average HashMap size is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for HashMapToKeyValPair&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>key</i>(K)</th><th><i>val</i>(V)</th><th><i>keyval</i>(HashMap&lt;K,V&gt;(1))</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5,c=1}</td><td>a ; b ; c</td><td>2 ; 5 ; 1</td><td>{a=2} ; {b=5} ; {c=1}</td></tr>
 * <tr><td>Data (process())</td><td>{}</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=4,z=5,d=66,b=1111,i=-1,a=33}</td><td>a ; z ; d ; b ; i ; a</td><td>4 ; 5 ; 66 ; 1111 ; -1 ; 33</td><td>{a=4} ; {z=5} ; {d=66} ; {b=1111} ; {i=-1} ; {a=33}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * <br>
 */

public class HashMapToKeyValPair<K, V> extends BaseKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>()
  {
    /**
     * Emits key, key/val pair, and val based on port connections
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        if (key.isConnected()) {
          key.emit(cloneKey(e.getKey()));
        }
        if (val.isConnected()) {
          val.emit(cloneValue(e.getValue()));
        }
        if (keyval.isConnected()) {
          keyval.emit(new KeyValPair<K, V>(cloneKey(e.getKey()), cloneValue(e.getValue())));
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "key", optional = true)
  public final transient DefaultOutputPort<K> key = new DefaultOutputPort<K>();
  @OutputPortFieldAnnotation(name = "keyval", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, V>> keyval = new DefaultOutputPort<KeyValPair<K, V>>();
  @OutputPortFieldAnnotation(name = "val", optional = true)
  public final transient DefaultOutputPort<V> val = new DefaultOutputPort<V>();
}
