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

import org.apache.apex.malhar.lib.util.BaseKeyValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;

/**
 * An implementation of BaseKeyValueOperator that converts Key Value Pair to a HashMap tuple.
 * <p>
 * Takes a KeyValPair and emits a HashMap(1), Used for for converting KeyValPair
 * to a HashMap(1) tuple
 * <p>
 * This is a pass through operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>keyval</b>: expects KeyValPair&lt;K,V&gt;<br>
 * <b>map</b>: emits HashMap&lt;K,V&gt;<br>
 * <br>
 * @displayName Key Value Pair To HashMap
 * @category Tuple Converters
 * @tags key value
 * @since 0.3.3
 */
@Stateless
public class KeyValPairToHashMap<K, V> extends BaseKeyValueOperator<K, V>
{
  /**
   * Input port that takes a key value pair.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> keyval = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * Emits key, key/val pair, and val based on port connections
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      HashMap<K, V> otuple = new HashMap<K, V>(1);
      otuple.put(tuple.getKey(), tuple.getValue());
      map.emit(otuple);
    }
  };

  /**
   * key/value map output port.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> map = new DefaultOutputPort<HashMap<K, V>>();
}
