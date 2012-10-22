/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.FailedOperationException;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.lib.util.MutableInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

/**
 *
 * Takes a stream of key value pairs via input port "data", and they are ordered by a given key. The ordered tuples are emitted on port "out_data" at the end of window<p>
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
public class OrderByKey<K, V> extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
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
          count = new MutableInteger(1);
          first = true;
        }
        else {
          count.value++;
        }
        countmap.put(val, count);
      }
      if (ordered_list.isConnected()) {
        ArrayList list = (ArrayList)smap.get(val);
        if (list == null) { // already in the queue
          list = new ArrayList();
          list.add(tuple);
          smap.put(val, list);
          first = true;
        }
        list.add(tuple);
      }
      if (first) {
        pqueue.add(val);
      }
    }
  };
  public final transient DefaultOutputPort<HashMap<K, V>> ordered_list = new DefaultOutputPort<HashMap<K, V>>(this);
  public final transient DefaultOutputPort<HashMap<V, Integer>> ordered_count = new DefaultOutputPort<HashMap<V, Integer>>(this);
  String orderby = "";
  protected PriorityQueue<V> pqueue = null;
  protected HashMap<V, MutableInteger> countmap = new HashMap<V, MutableInteger>();
  protected HashMap<V, ArrayList<HashMap<K, V>>> smap = new HashMap<V, ArrayList<HashMap<K, V>>>();

  public void setOrderby(String str)
  {
    orderby = str;
  }

  public PriorityQueue<V> initializePriorityQueue() {
    return new PriorityQueue<V>(5);
  }

  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException
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
      pqueue = initializePriorityQueue();
    }
    pqueue.clear();
    countmap.clear();
    smap.clear();
  }

  /**
   *
   */
  @Override
  public void endWindow()
  {
    V key;
    while ((key = pqueue.poll()) != null) {
      if (ordered_count.isConnected()) {
        HashMap<V, Integer> tuple = new HashMap<V, Integer>(1);
        tuple.put(key, countmap.get(key).value);
        ordered_count.emit(tuple);
      }
      if (ordered_list.isConnected()) {
        ArrayList<HashMap<K, V>> list = (ArrayList<HashMap<K, V>>)smap.get(key);
        for (HashMap<K, V> o: list) {
          ordered_list.emit(o);
        }
      }
    }
  }
}
