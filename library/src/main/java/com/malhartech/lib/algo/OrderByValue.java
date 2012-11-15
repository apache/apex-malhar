/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 *
 * Order by ascending on value is done on the incoming stream based on key, and result is emitted on end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String,Integer><br>
 * <b>out_data</b>: Output data port, emits HashMap<String,Integer><br>
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
public class OrderByValue<K,V> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * Processes each tuple, and orders by the given value
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
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
  @OutputPortFieldAnnotation(name = "ordered_list")
  public final transient DefaultOutputPort<HashMap<K,V>> ordered_list = new DefaultOutputPort<HashMap<K,V>>(this);
  @OutputPortFieldAnnotation(name = "ordered_count")
  public final transient DefaultOutputPort<HashMap<K,HashMap<V,Integer>>> ordered_count = new DefaultOutputPort<HashMap<K, HashMap<V,Integer>>>(this);
  PriorityQueue<V> pqueue = new PriorityQueue<V>(5);
  HashMap<V,HashMap<K,MutableInteger>> smap = new HashMap<V,HashMap<K,MutableInteger>>();


  /**
   * Initializes the priority queue in ascending order
   * @return consructed PriorityQueue
   */
  public PriorityQueue<V> initializePriorityQueue()
  {
    return new PriorityQueue<V>(5);
  }

  /**
   * Sets up the priority queue
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    initializePriorityQueue();
  }

  /**
   * Clears cache/hash
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    if (pqueue == null) {
      initializePriorityQueue();
    }
    pqueue.clear();
    smap.clear();
  }

  /**
   * Emits tuples
   */
  @Override
  public void endWindow()
  {
    V ival;
    while ((ival = pqueue.poll()) != null) {
      HashMap<K,MutableInteger> istr = smap.get(ival);
      if (istr == null) { // Should never be null
        continue;
      }
      for (Map.Entry<K, MutableInteger> e: istr.entrySet()) {
        final int count = e.getValue().value;
        if (ordered_list.isConnected()) {
          for (int i = 0; i < count; i++) {
            HashMap<K,V> tuple = new HashMap<K,V>(1);
            tuple.put(e.getKey(), ival);
            ordered_list.emit(tuple);
          }
        }
        if (ordered_count.isConnected()) {
          HashMap<K,HashMap<V,Integer>> tuple = new HashMap<K,HashMap<V,Integer>>(1);
          HashMap<V,Integer> data = new HashMap<V,Integer>(1);
          data.put(ival, new Integer(count));
          tuple.put(e.getKey(), data);
          ordered_count.emit(tuple);
        }
      }
    }
  }
}
