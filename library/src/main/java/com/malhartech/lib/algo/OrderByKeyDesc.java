/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.ReversibleComparator;
import java.util.PriorityQueue;

/**
 *
 * Order by descending is done on the incoming stream based on key, and result is emitted on end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Object><br>
 * <b>ordered_count</b>: emits HashMap<Object, Integer><br>
 * <b>ordered_list</b>: Output data port, emits ArrayList<HashMap<String, Object>><br>
 * <b>Properties</b>:
 * <b>orderby</b>: The key to order by<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * Parameter "key" cannot be empty<br>
 * <br>
 * Run time checks are:<br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class OrderByKeyDesc<K, V> extends OrderByKey<K, V>
{
  @Override
  public PriorityQueue<V> initializePriorityQueue()
  {
    return new PriorityQueue<V>(5, new ReversibleComparator<V>(false));
  }
}
