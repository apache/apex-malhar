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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the base implementation of an operator,
 * which orders tuples per key and emits the top N tuples per key at the end of the window.&nbsp;
 * Subclasses should implement the methods which control the ordering and emission of tuples.
 * <p>
 * Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap&lt;K,V&gt; (&lt;key, value&gt;)<br>
 * <b>bottom</b>: Output data port, emits HashMap&lt;K,ArrayList&lt;V&gt;&gt; (&lt;key, ArrayList&lt;values&gt;&gt;)<br>
 * <b>Properties</b>:
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * N: Has to be an integer<br>
 * <br>
 * Run time checks are:<br>
 * </p>
 * @displayName Abstract Base N Non Unique Map
 * @category Algorithmic
 * @tags rank
 * @since 0.3.2
 */
public abstract class AbstractBaseNNonUniqueOperatorMap<K, V> extends AbstractBaseNOperatorMap<K, V>
{
  /**
   * Override to decide the direction (ascending vs descending)
   * @return true if ascending, to be done by sub-class
   */
  public abstract boolean isAscending();

  /**
   * Override to decide which port to emit to and its schema
   * @param tuple
   */
  public abstract void emit(HashMap<K, ArrayList<V>> tuple);

  /**
   *
   * Inserts tuples into the queue
   * @param tuple to insert in the queue
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void processTuple(Map<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      TopNSort pqueue = kmap.get(e.getKey());
      if (pqueue == null) {
        pqueue = new TopNSort<V>(5, n, isAscending());
        kmap.put(cloneKey(e.getKey()), pqueue);
        pqueue.offer(cloneValue(e.getValue()));
      } else {
        pqueue.offer(e.getValue());
      }
    }
  }

  protected HashMap<K, TopNSort<V>> kmap = new HashMap<K, TopNSort<V>>();

  /**
   * Emits the result
   * Clears the internal data
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, TopNSort<V>> e: kmap.entrySet()) {
      HashMap<K, ArrayList<V>> tuple = new HashMap<K, ArrayList<V>>(1);
      tuple.put(e.getKey(), (ArrayList<V>)e.getValue().getTopN(getN()));
      emit(tuple);
    }
    kmap.clear();
  }
}
