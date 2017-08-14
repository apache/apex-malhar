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
import java.util.PriorityQueue;
import javax.validation.constraints.Min;
import org.apache.commons.lang.mutable.MutableInt;

/**
 * This is the base implementation of an operator,
 * which can emit either a map or list of tuples at the end of each application window.&nbsp;
 * If the operator emits a list, the list contains the tuples ordered by the their counts.&nbsp;
 * If the operator emits a map, it contains the tuples mapped to their corresponding counts.&nbsp;
 * Subclasses should implement the methods used to emit lists and maps.
 * <p></p>
 * @displayName Abstract Base Sort
 * @category Algorithmic
 * @tags rank, count
 * @since 0.3.2
 */
//
// TODO: Override PriorityQueue and rewrite addAll to insert with location
//
public abstract class AbstractBaseSortOperator<K> extends BaseKeyOperator<K>
{

  /**
   * Calls processTuple(K) for each item in the list. Override if you need to do this more optimally
   * @param tuple
   */
  public void processTuple(ArrayList<K> tuple)
  {
    for (K e: tuple) {
      processTuple(e);
    }
  }

  /**
   * Inserts each unique key. Updates ref count if key is not unique
   * @param e
   */
  public void processTuple(K e)
  {
    if (e == null) {
      return;
    }

    MutableInt count = pmap.get(e);
    if (count == null) {
      count = new MutableInt(0);
      pmap.put(e, count);
      pqueue.add(e);
    }
    count.increment();
  }

  @Min(1)
  int size = 10;
  protected PriorityQueue<K> pqueue = null;
  protected HashMap<K, MutableInt> pmap = new HashMap<K, MutableInt>();

  /**
   * getter function for size
   * @return size
   */
  @Min(1)
  public int getSize()
  {
    return size;
  }

  /**
   * Set a size to avoid needing the queue to grow. The queue is purged per window, so the size should be set
   * in sync with number of unique tuples expected per window
   *
   * @param val
   */
  public void setSize(int val)
  {
    size = val;
  }

  /**
   * Initializes queue if not done (once at the start)
   */
  @Override
  public void beginWindow(long windowId)
  {
    if (pqueue == null) {
      initializeQueue();
    }
  }

  public void initializeQueue()
  {
    pqueue = new PriorityQueue<K>(getSize());
  }


  public abstract boolean doEmitList();

  public abstract boolean doEmitHash();

  public abstract void emitToList(ArrayList<K> list);

  public abstract void emitToHash(HashMap<K,Integer> map);

  /**
   * Emit sorted tuple at end of window
   * Clears internal cache
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void endWindow()
  {
    if (pqueue.isEmpty()) {
      return;
    }
    ArrayList tuple = new ArrayList();
    HashMap<K, Integer> htuple = new HashMap<K, Integer>(pqueue.size());
    final boolean sok = doEmitList();
    final boolean hok = doEmitHash();

    if (sok) {
      tuple = new ArrayList();
    }

    if (hok) {
      htuple = new HashMap<K, Integer>(pqueue.size());
    }

    K o;
    while ((o = pqueue.poll()) != null) {
      MutableInt count = pmap.get(o);
      if (sok) {
        for (int i = 0; i < count.intValue(); i++) {
          tuple.add(cloneKey(o));
        }
      }

      if (hok) {
        htuple.put(cloneKey(o), count.intValue());
      }
    }
    if (sok) {
      emitToList(tuple);
    }
    if (hok) {
      emitToHash(htuple);
    }
    pmap.clear();
    pqueue.clear();
  }
}
