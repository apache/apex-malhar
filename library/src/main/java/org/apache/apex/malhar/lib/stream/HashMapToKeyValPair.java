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
package org.apache.apex.malhar.lib.stream;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.util.BaseKeyValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * An implementation of BaseKeyValueOperator that breaks a HashMap tuple into objects.
 * <p>
 * Takes a HashMap and emits its keys, keyvals, vals; used for breaking up a
 * HashMap tuple into objects (keys, vals, or &lt;key,val&gt; pairs)
 * <p>
 * This is a pass through operator<br>
 * <br>
 * <b>StateFull : No</b> <br>
 * <b>Partitions : Yes </b><br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>key</b>: emits K<br>
 * <b>keyval</b>: emits Entry&lt;K,V&gt;<br>
 * <b>val</b>: emits V<br>
 * <br>
 * @displayName Hash Map To Key Value Pair
 * @category Tuple Converters
 * @tags hashmap, key value
 * @since 0.3.3
 */
@Stateless
public class HashMapToKeyValPair<K, V> extends BaseKeyValueOperator<K, V>
{
  /**
   * Input port that takes a hashmap of &lt;key,value&rt;.
   */
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>()
  {
    /**
     * Emits key, key/val pair, and val based on port connections
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e : tuple.entrySet()) {
        if (key.isConnected()) {
          key.emit(cloneKey(e.getKey()));
        }
        if (val.isConnected()) {
          val.emit(cloneValue(e.getValue()));
        }
        if (keyval.isConnected()) {
          keyval.emit(new KeyValPair<K, V>(cloneKey(e.getKey()), cloneValue(e
              .getValue())));
        }
      }
    }
  };

  /**
   * Key output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<K> key = new DefaultOutputPort<K>();

  /**
   * key/value pair output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, V>> keyval = new DefaultOutputPort<KeyValPair<K, V>>();

  /**
   * Value output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<V> val = new DefaultOutputPort<V>();
}
