/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract class for sorting NUnique key, val pairs, emit is done at end of window<p>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 *
 * @since 0.3.2
 */
public abstract class AbstractBaseNUniqueOperatorMap<K, V> extends AbstractBaseNOperatorMap<K, V>
{
  HashMap<K, TopNUniqueSort<V>> kmap = new HashMap<K, TopNUniqueSort<V>>();

  /**
   * Override to decide the direction (ascending vs descending)
   * @return true if ascending
   */
  abstract public boolean isAscending();

  /**
   * Override to decide which port to emit to and its schema
   * @param tuple
   */
  abstract public void emit(HashMap<K, ArrayList<HashMap<V,Integer>>> tuple);

  /**
   * Inserts tuples into the queue
   *
   * @param tuple to insert in the queue
   */
  @Override
  public void processTuple(Map<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {

      TopNUniqueSort<V> pqueue = kmap.get(e.getKey());
      if (pqueue == null) {
        pqueue = new TopNUniqueSort<V>(5, n, isAscending());
        kmap.put(cloneKey(e.getKey()), pqueue);
        pqueue.offer(cloneValue(e.getValue()));
      }
      else {
        pqueue.offer(cloneValue(e.getValue()));
      }
    }
  }

  /**
   * Emits the result
   */
  @SuppressWarnings("unchecked")
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, TopNUniqueSort<V>> e: kmap.entrySet()) {
      HashMap<K, ArrayList<HashMap<V,Integer>>> tuple = new HashMap<K, ArrayList<HashMap<V,Integer>>>(1);
      tuple.put(e.getKey(), e.getValue().getTopN(getN()));
      emit(tuple);
    }
    kmap.clear();
  }
}
