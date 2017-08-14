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

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.util.BaseKeyOperator;
import org.apache.apex.malhar.lib.util.UnifierHashMap;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * This operator filters the incoming stream of key value pairs based on the keys specified by property "keys"..
 * <p>
 * Filters the incoming stream based of keys specified by property "keys". If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted
 * </p>
 * <p>
 * Operator assumes that the key, val pairs are immutable objects. If this operator has to be used for mutable objects,
 * override "cloneKey()" to make copy of K, and "cloneValue()" to make copy of V.<br>
 * This is a pass through node<br>
 * <br>
 * <b>StateFull : No, </b> tuple are processed in current window. <br>
 * <b>Partitions : Yes, </b> no dependency among input tuples. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Expects Map&lt;K,V&gt;<br>
 * <b>filter</b>: Emits HashMap&lt;K,V&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>keys</b>: The keys to pass through, rest are filtered/dropped. A comma separated list of keys<br>
 * <br>
 * </p>
 *
 * @displayName Filter Keyval Pairs By Key Generic
 * @category Rules and Alerts
 * @tags filter, key value
 *
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
@Stateless
@OperatorAnnotation(partitionable = true)
public class FilterKeysMap<K,V> extends BaseKeyOperator<K>
{
  /**
   * Filter keys map.
   */
  @NotNull()
  HashMap<K, V> keys = new HashMap<K, V>();

  /**
   * Emits key not in filter map.
   */
  boolean inverse = false;

  /**
   * The input port on which key value pairs are received.
   */
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Processes incoming tuples one key,val at a time. Emits if at least one key makes the cut
     * By setting inverse as true, match is changed to un-matched
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      HashMap<K, V> dtuple = null;
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        boolean contains = keys.containsKey(e.getKey());
        if ((contains && !inverse) || (!contains && inverse)) {
          if (dtuple == null) {
            dtuple = new HashMap<K, V>(4); // usually the filter keys are very few, so 4 is just fine
          }
          dtuple.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
        }
      }
      if (dtuple != null) {
        filter.emit(dtuple);
      }
    }
  };

  /**
   * The output port on which filtered key value pairs are emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> filter = new DefaultOutputPort<HashMap<K, V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return new UnifierHashMap<K, V>();
    }
  };

  /**
   * If true then only matches are emitted. If false then only non matches are emitted.
   * @return inverse
   */
  public boolean getInverse()
  {
    return inverse;
  }


  /**
   * Sets the inverse property. If true then only matches are emitted. If false then only non matches are emitted.
   * @param val
   */
  public void setInverse(boolean val)
  {
    inverse = val;
  }

  /**
   * Adds a key to the filter list
   * @param str
   */
  public void setKey(K str)
  {
    keys.put(str, null);
  }

  /**
   * Adds the list of keys to the filter list
   * @param list
   */
  public void setKeys(K[] list)
  {
    if (list != null) {
      for (K e: list) {
        keys.put(e, null);
      }
    }
  }

  /**
   * The keys to filter. The values in the map should be null.
   * @param keys
   */
  public void setKeys(HashMap<K, V> keys)
  {
    this.keys = keys;
  }

  /**
   * Gets the keys to filter.
   * @return Returns a map containing the keys.
   */
  public HashMap<K, V> getKeys()
  {
    return keys;
  }

  /*
   * Clears the filter list
   */
  public void clearKeys()
  {
    keys.clear();
  }

  /**
   * Clones V object. By default assumes immutable object (i.e. a copy is not made). If object is mutable, override this method
   * @param v value to be cloned
   * @return returns v as is (assumes immutable object)
   */
  public V cloneValue(V v)
  {
    return v;
  }
}
