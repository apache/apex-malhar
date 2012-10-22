/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 *
 * Takes a stream of key value pairs via input port "data", and they are ordered by their value. The ordered tuples are emitted on port "out_data" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Integer><br>
 * <b>out_data</b>: Output data port, emits HashMap<String, Integer><br>
 * <b>Properties</b>:
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <br>
 * Run time checks are:<br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class OrderByValue<K, V> extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        HashMap<K, MutableInteger> istr = smap.get(e.getValue());
        if (istr == null) { // not in priority queue
          istr = new HashMap<K, MutableInteger>(4);
          istr.put(e.getKey(), new MutableInteger(1));
          smap.put(e.getValue(), istr);
          pqueue.add(e.getValue());
        }
        else { // value is in the priority queue
          MutableInteger scount = istr.get(e.getKey());
          if (scount == null) { // this key does not exist
            istr.put(e.getKey(), new MutableInteger(0));
          }
          scount.value++;
        }
      }
    }
  };
  public final transient DefaultOutputPort<HashMap<K, V>> ordered_list = new DefaultOutputPort<HashMap<K, V>>(this);
  public final transient DefaultOutputPort<HashMap<K, HashMap<V, Integer>>> ordered_count = new DefaultOutputPort<HashMap<K, HashMap<V, Integer>>>(this);
  PriorityQueue<V> pqueue = new PriorityQueue<V>(5);
  HashMap<V, HashMap<K, MutableInteger>> smap = new HashMap<V, HashMap<K, MutableInteger>>();


  public PriorityQueue<V> initializePriorityQueue()
  {
    return new PriorityQueue<V>(5);
  }

  @Override
  public void setup(OperatorConfiguration config)
  {
    initializePriorityQueue();
  }
  /**
   * Cleanup at the start of window
   */
  @Override
  public void beginWindow()
  {
    if (pqueue == null) {
      initializePriorityQueue();
    }
    pqueue.clear();
    smap.clear();
  }

  /**
   *
   */
  @Override
  public void endWindow()
  {
    V ival;
    while ((ival = pqueue.poll()) != null) {
      HashMap<K, MutableInteger> istr = smap.get(ival);
      if (istr == null) { // Should never be null
        continue;
      }
      for (Map.Entry<K, MutableInteger> e: istr.entrySet()) {
        final int count = e.getValue().value;
        if (ordered_list.isConnected()) {
          for (int i = 0; i < count; i++) {
            HashMap<K, V> tuple = new HashMap<K, V>(1);
            tuple.put(e.getKey(), ival);
            ordered_list.emit(tuple);
          }
        }
        if (ordered_count.isConnected()) {
          HashMap<K, HashMap<V, Integer>> tuple = new HashMap<K, HashMap<V, Integer>>(1);
          HashMap<V, Integer> data = new HashMap<V, Integer>(1);
          data.put(ival, new Integer(count));
          tuple.put(e.getKey(), data);
          ordered_count.emit(tuple);
        }
      }
    }
  }
}
