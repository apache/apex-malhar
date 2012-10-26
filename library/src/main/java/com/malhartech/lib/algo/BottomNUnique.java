/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.TopNUniqueSort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Takes a stream of key value pairs via input port "data", and they are ordered by key. Bottom N of the ordered unique tuples per key are emitted on
 * port "top" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<K,V> (<key, value>)<br>
 * <b>top</b>: Output data port, emits HashMap<K, ArrayList<V>> (<key, ArrayList<value>>)<br>
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
public class BottomNUnique<K, V> extends BaseNOperator<K, V>
{
  /**
   * Inserts tuples into the queue
   * @param tuple to insert in the queue
   */
  @Override
  public void processTuple(HashMap<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      TopNUniqueSort pqueue = kmap.get(e.getKey());
      if (pqueue == null) {
        pqueue = new TopNUniqueSort<V>(5, n, false);
        kmap.put(e.getKey(), pqueue);
      }
      pqueue.offer(e.getValue());
    }
  }
  /**
   * Emits a HashMap<K,ArrayList<V>> tuple. ArrayList are the BottomNUnique values
   */
  @OutputPortFieldAnnotation(name = "bottom")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> bottom = new DefaultOutputPort<HashMap<K, ArrayList<V>>>(this);

  private HashMap<K, TopNUniqueSort<V>> kmap = new HashMap<K, TopNUniqueSort<V>>();

  /**
   * Cleans up the cache to start anew
   */
  @Override
  public void beginWindow()
  {
    kmap.clear();
  }

  /**
   * Emits a HashMap<K,ArrayList<V>> tuple. ArrayList are the BottomNUnique values
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, TopNUniqueSort<V>> e: kmap.entrySet()) {
      HashMap<K, ArrayList<V>> tuple = new HashMap<K, ArrayList<V>>(1);
      tuple.put(e.getKey(), e.getValue().getTopN());
      bottom.emit(tuple);
    }
  }
}
