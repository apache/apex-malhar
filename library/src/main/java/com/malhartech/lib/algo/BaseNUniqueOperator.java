/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.TopNUniqueSort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for sorting NUnique key, val pairs, emit is done at end of window<p>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * @author amol<br>
 *
 */
public abstract class BaseNUniqueOperator<K, V> extends BaseNOperator<K, V>
{
  HashMap<K, TopNUniqueSort<V>> kmap = new HashMap<K, TopNUniqueSort<V>>();

  /**
   * Override to decide the direction (ascending vs descending)
   * @return
   */
  abstract public boolean isAscending();

  /**
   * Override to decide which port to emit to and its schema
   * @param tuple
   */
  abstract void emit(HashMap<K, ArrayList<V>> tuple);

  /**
   * Inserts tuples into the queue
   *
   * @param tuple to insert in the queue
   */
  @Override
  public void processTuple(HashMap<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {

      TopNUniqueSort<V> pqueue = kmap.get(e.getKey());
      if (pqueue == null) {
        pqueue = new TopNUniqueSort<V>(5, n, isAscending());
        kmap.put(cloneKey(e.getKey()), pqueue);
        pqueue.offer(cloneValue(e.getValue()));
      }
      else {
        pqueue.offer(e.getValue());
      }
    }
  }

  /**
   * Clears cache to start fresh
   */
  @Override
  public void beginWindow(long windowId)
  {
    kmap.clear();
  }


  /**
   * Emits the result
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, TopNUniqueSort<V>> e: kmap.entrySet()) {
      HashMap<K, ArrayList<V>> tuple = new HashMap<K, ArrayList<V>>(1);
      tuple.put(e.getKey(), e.getValue().getTopN(getN()));
      emit(tuple);
    }
  }
}
