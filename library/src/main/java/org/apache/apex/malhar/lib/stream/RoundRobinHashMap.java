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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * <p>
 * An implementation of BaseKeyValueOperator that creates a HashMap tuple from incoming tuples.
 * <p>
 * If keys[] are set, then each incoming tuple is the value of the key (in-order) till all keys get a value.
 * Once all keys are assigned values, the tuple (HashMap) is emitted, the
 * process of assigning values starts again<br>
 * This is a stateful operator as it waits across window boundary to complete
 * the HashTable<br>
 * <br>
 * <b> StateFull : Yes, </b>Operator maintains index of key across windows. <br>
 * <b> Partitions : Yes </b> <br>
 * <br>
 * <b>Port</b>:<br>
 * <b>data</b>: expects V<br>
 * <b>map</b>: emits HashMap&lt;K,v&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>keys[]</b>: Set of keys to insert in the output tuple</b>
 * @displayName Round Robin HashMap
 * @category Tuple Converters
 * @tags key value, hashmap
 * @since 0.3.3
 */
public class RoundRobinHashMap<K, V> extends BaseKeyValueOperator<K, V>
{
  /**
   * Keys for round robin association.
   */
  protected K[] keys;

  /**
   * Current key index.
   */
  protected int cursor = 0;

  private HashMap<K, V> otuple;

  /**
   * Value input port.
   */
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Emits key, key/val pair, and val based on port connections
     */
    @Override
    public void process(V tuple)
    {
      if (keys.length == 0) {
        return;
      }
      if (cursor == 0) {
        otuple = new HashMap<K, V>();
      }
      otuple.put(keys[cursor], tuple);
      if (++cursor >= keys.length) {
        map.emit(otuple);
        cursor = 0;
        otuple = null;
      }
    }
  };

  /**
   * key/value map output port.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> map = new DefaultOutputPort<HashMap<K, V>>();

  /**
   * Keys for round robin asspociation, set by application.
   *
   * @param keys
   */
  public void setKeys(K[] keys)
  {
    this.keys = keys;
  }
}
