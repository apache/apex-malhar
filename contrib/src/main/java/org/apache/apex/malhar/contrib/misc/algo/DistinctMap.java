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
import java.util.Map;

import org.apache.apex.malhar.lib.util.BaseKeyValueOperator;
import org.apache.apex.malhar.lib.util.UnifierHashMap;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator computes and emits distinct key,val pairs (i.e drops duplicates).
 * <p>
 * Computes and emits distinct key,val pairs (i.e drops duplicates)
 * </p>
 * <p>
 * This is a pass through operator<br>
 * <br>
 * This module is same as a "FirstOf" metric on any key,val pair. At end of window all data is flushed.<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compare across application window(s). <br>
 * <b>Partitions : Yes, </b> distinct output is unified by unifier hash map operator. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input data port expects Map&lt;K,V&gt;<br>
 * <b>distinct</b>: Output data port, emits HashMap&lt;K,V&gt;(1)<br>
 * <br>
 * </p>
 *
 * @displayName Distinct Key Value Merge
 * @category Stream Manipulators
 * @tags filter, unique, key value
 *
 * @since 0.3.2
 * @deprecated
 */

@Deprecated
@OperatorAnnotation(partitionable = true)
public class DistinctMap<K, V> extends BaseKeyValueOperator<K, V>
{
  /**
   * The input port on which key value pairs are received.
   */
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Process HashMap<K,V> tuple on input port data, and emits if match not found. Updates the cache
     * with new key,val pair
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        HashMap<V, Object> vals = mapkeyval.get(e.getKey());
        if ((vals == null) || !vals.containsKey(e.getValue())) {
          HashMap<K, V> otuple = new HashMap<K, V>(1);
          otuple.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
          distinct.emit(otuple);
          if (vals == null) {
            vals = new HashMap<V, Object>();
            mapkeyval.put(cloneKey(e.getKey()), vals);
          }
          vals.put(cloneValue(e.getValue()), null);
        }
      }
    }
  };

  /**
   * The output port on which distinct key value pairs are emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> distinct = new DefaultOutputPort<HashMap<K, V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return new UnifierHashMap<K, V>();
    }
  };


  protected HashMap<K, HashMap<V, Object>> mapkeyval = new HashMap<K, HashMap<V, Object>>();

  /**
   * Clears the cache/hash
   */
  @Override
  public void endWindow()
  {
    mapkeyval.clear();
  }
}
