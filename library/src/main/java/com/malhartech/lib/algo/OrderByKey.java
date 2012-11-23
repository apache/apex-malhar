/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyValueOperator;
import com.malhartech.lib.util.MutableInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import javax.validation.constraints.NotNull;

/**
 * TBD: Not certified yet
 * Order by ascending is done on the incoming stream based on val, and result is emitted on end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: expects HashMap&lt;String,V&gt;<br>
 * <b>ordered_count</b>: emits HashMap&lt;V,Integer&gt;<br>
 * <b>ordered_list</b>: Output data port, emits ArrayList&lt;HashMap&lt;String,Object&gt;&gt;<br>
 * <b>Properties</b>:
 * <b>orderby</b>: The val to order by<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * Parameter "val" cannot be empty<br>
 * <br>
 * Run time checks are:<br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class OrderByKey<K,V> extends BaseKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * process the tuple is orderby val exists.
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      V val = tuple.get(orderby);
      if (val == null) {
        // emit error tuple?
        return;
      }
      boolean first = false;
      if (ordered_count.isConnected()) {
        MutableInteger count = countmap.get(val);
        if (count == null) {
          count = new MutableInteger(0);
          countmap.put(cloneValue(val), count);
          first = true;
        }
        count.value++;
      }
      if (ordered_list.isConnected()) {
        ArrayList<HashMap<K,V>> list = smap.get(val);
        if (list == null) {
          list = new ArrayList<HashMap<K,V>>();
          smap.put(cloneValue(val), list);
          first = true;
        }
        list.add(cloneTuple(tuple));
      }
      if (first) {
        pqueue.add(cloneValue(val));
      }
    }
  };

  @OutputPortFieldAnnotation(name = "ordered_list")
  public final transient DefaultOutputPort<HashMap<K,V>> ordered_list = new DefaultOutputPort<HashMap<K,V>>(this);
  @OutputPortFieldAnnotation(name = "ordered_count")
  public final transient DefaultOutputPort<HashMap<V,Integer>> ordered_count = new DefaultOutputPort<HashMap<V,Integer>>(this);

  @NotNull()
  K orderby = null;
  protected PriorityQueue<V> pqueue = null;
  protected HashMap<V,MutableInteger> countmap = new HashMap<V,MutableInteger>();
  protected HashMap<V,ArrayList<HashMap<K,V>>> smap = new HashMap<V,ArrayList<HashMap<K,V>>>();

  /**
   * getter function for orderby
   * @return orderby
   */
  @NotNull()
  public K getOrderby()
  {
    return orderby;
  }

  /**
   * setter function for orderby val
   * @param str
   */
  public void setOrderby(K str)
  {
    orderby = str;
  }

  /**
   * First cut of the priority queue in ascending order
   * @return constructed PriorityQueue
   */
  public void initializePriorityQueue() {
    pqueue = new PriorityQueue<V>(5);
  }

  /**
   * Sets up the priority queue
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    if (getOrderby() == null) {
      throw new IllegalArgumentException("Orderby key not set");
    }
    initializePriorityQueue();
  }

  /**
   * Cleanup at the start of window
   */
  @Override
  public void beginWindow(long windowId)
  {
    if (pqueue == null) {
      initializePriorityQueue();
    }
    pqueue.clear();
    countmap.clear();
    smap.clear();
  }

  /**
   * Emits tuples
   */
  @Override
  public void endWindow()
  {
    V val;
    while ((val = pqueue.poll()) != null) {
      if (ordered_count.isConnected()) {
        HashMap<V, Integer> tuple = new HashMap<V, Integer>(1);
        tuple.put(val, countmap.get(val).value);
        ordered_count.emit(tuple);
      }
      if (ordered_list.isConnected()) {
        ArrayList<HashMap<K, V>> list = smap.get(val);
        for (HashMap<K, V> o: list) {
          ordered_list.emit(o);
        }
      }
    }
  }
}
