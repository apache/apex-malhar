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
 * Takes a stream of key value pairs via input port "data", and they are ordered by key. Top N of the ordered unique tuples per key are emitted on
 * port "top" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Object> (key, value)<br>
 * <b>top</b>: Output data port, emits HashMap<String, ArrayList<HashMap<Object, Integer>>> (key, <value, Integer>)br>
 * <b>Properties</b>:
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * N: Has to be an integer<br>
 * <br>
 * Run time checks are:<br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class TopNUnique<K, V> extends BaseTopN<K, V>
{
  HashMap<K, TopNUniqueSort<V>> kmap = new HashMap<K, TopNUniqueSort<V>>();

  @Override
  public void insertIntoQueue(K k, V v)
  {
    TopNUniqueSort<V> pqueue = kmap.get(k);
    if (pqueue == null) {
      pqueue = new TopNUniqueSort<V>(5, n, true);
      kmap.put(k, pqueue);
    }
    pqueue.offer(v);
  }

  @Override
  public void beginWindow()
  {
    kmap.clear();
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<K, TopNUniqueSort<V>> e: kmap.entrySet()) {
      HashMap<K, ArrayList<V>> tuple = new HashMap<K, ArrayList<V>>(1);
      tuple.put(e.getKey(), e.getValue().getTopN());
      top.emit(tuple);
    }
  }
}
