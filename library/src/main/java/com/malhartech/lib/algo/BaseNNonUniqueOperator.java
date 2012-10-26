/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.TopNSort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Takes a stream of key value pairs via input port "data", and they are ordered by key. Bottom N of the ordered tuples per key are emitted on
 * port "bottom" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<StriK,V> (<key, value><br>
 * <b>bottom</b>: Output data port, emits HashMap<K, ArrayList<V>> (<key, ArraList<values>>)<br>
 * <b>Properties</b>:
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * N: Has to be an integer<br>
 * <br>
 * Run time checks are:<br>
 * <br>
 * @author amol<br>
 *
 */
public abstract class BaseNNonUniqueOperator<K, V> extends BaseNOperator<K,V>
{
  abstract public boolean isAscending();
  abstract void emit(HashMap<K, ArrayList<V>> tuple);

  /**
   * Inserts tuples into the queue
   * @param tuple to insert in the queue
   */
  @Override
  public void processTuple(HashMap<K, V> tuple)
  {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        TopNSort pqueue = kmap.get(e.getKey());
        if (pqueue == null) {
          pqueue = new TopNSort<V>(5, n, isAscending());
          kmap.put(e.getKey(), pqueue);
        }
        pqueue.offer(e.getValue());
      }
  }

  HashMap<K, TopNSort<V>> kmap = new HashMap<K, TopNSort<V>>();

  @Override
  public void beginWindow()
  {
    kmap.clear();
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<K, TopNSort<V>> e: kmap.entrySet()) {
      HashMap<K, ArrayList<V>> tuple = new HashMap<K, ArrayList<V>>(1);
      tuple.put(e.getKey(), e.getValue().getTopN(getN()));
      emit(tuple);
    }
  }
}
