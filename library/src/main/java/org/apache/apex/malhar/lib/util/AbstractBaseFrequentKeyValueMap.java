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
package org.apache.apex.malhar.lib.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.DefaultInputPort;

/**
 * This is the base implementation of an operator, which takes key value pairs as inputs.&nbsp;
 * It counts the number of times each key value pair occurs.&nbsp;
 * The counts of each key value pair are compared and the winning key value pair(s) for each key is emitted.&nbsp;
 * Subclasses should implement the method that is used to emit key value pairs, as well as the comparison method used
 * to determine the winning key value pair(s).
 * <p>
 * This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects Map<K, V><br>
 * </p>
 * @displayName Emit Winning Key Value Pair(s)
 * @category Algorithmic
 * @tags count, compare, key value
 * @since 0.3.2
 */
public abstract class AbstractBaseFrequentKeyValueMap<K, V> extends BaseKeyValueOperator<K, V>
{
  /**
   * This is the input port which receives key value pairs.
   */
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Process every tuple to count occurrences of key,val pairs
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        HashMap<V, MutableInt> vals = keyvals.get(e.getKey());
        if (vals == null) {
          vals = new HashMap<V, MutableInt>(4);
          keyvals.put(cloneKey(e.getKey()), vals);
        }
        MutableInt count = vals.get(e.getValue());
        if (count == null) {
          count = new MutableInt(0);
          vals.put(cloneValue(e.getValue()), count);
        }
        count.increment();
      }
    }
  };
  HashMap<K, HashMap<V, MutableInt>> keyvals = new HashMap<K, HashMap<V, MutableInt>>();

  /**
   * Override compareCount to decide most vs least
   *
   * @param val1
   * @param val2
   * @return result of compareValue to be done by sub-class
   */
  public abstract boolean compareValue(int val1, int val2);

  /**
   * override emitTuple to decide the port to emit to
   *
   * @param tuple
   */
  public abstract void emitTuple(HashMap<K, HashMap<V, Integer>> tuple);

  /**
   * Emits the result.
   */
  @Override
  public void endWindow()
  {
    HashMap<V, Object> vmap = new HashMap<V, Object>();
    for (Map.Entry<K, HashMap<V, MutableInt>> e: keyvals.entrySet()) {
      V val = null;
      int kval = -1;
      vmap.clear();
      HashMap<V, MutableInt> vals = e.getValue();
      for (Map.Entry<V, MutableInt> v: vals.entrySet()) {
        if (kval == -1) {
          val = v.getKey();
          kval = v.getValue().intValue();
          vmap.put(val, null);
        } else if (compareValue(v.getValue().intValue(), kval)) {
          val = v.getKey();
          kval = v.getValue().intValue();
          vmap.clear();
          vmap.put(val, null);
        } else if (v.getValue().intValue() == kval) {
          vmap.put(v.getKey(), null);
        }
      }
      if ((val != null) && (kval > 0)) { // key is null if no
        HashMap<K, HashMap<V, Integer>> tuple = new HashMap<K, HashMap<V, Integer>>(1);
        HashMap<V, Integer> valpair = new HashMap<V, Integer>();
        for (Map.Entry<V, Object> v: vmap.entrySet()) {
          valpair.put(v.getKey(), new Integer(kval));
        }
        tuple.put(e.getKey(), valpair);
        emitTuple(tuple);
      }
    }
    keyvals.clear();
  }
}
