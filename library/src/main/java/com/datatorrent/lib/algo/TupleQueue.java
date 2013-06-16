/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.Min;

/**
 * <b>A demo operator. Needs to be moved to demo library</b>
 * Retains the last N values on any key, in effect acts like a fifo. The node also provides a lookup via port <b>lookup</b>. This module
 * was mainly developed for a demo<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>query</b>: expects K<br>
 * <b>queue</b>: emits the V that pops on insert<br>
 * <b>console</b>: emits HashMap&lt;K, ArrayList&lt;V&gt;&gt;, the current queue<br>
 * <br>
 * <b>Propertoes</b>:
 * <b>depth</b>: The depth of the queue. The number of objects to be retainrd<br>
 * <br>
 * <b>Benchmarks</b>: Not done as this operator was mainly developed for a demo<br>
 * <br>
 * Compile time checks:<br>
 * depth has to be an integer<br>
 * <br>
 * <br>
 *
 * @author amol
 */
public class TupleQueue<K,V> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * Process tuple to create a queue (FIFO)
     * Emits only if depth is reached
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        K key = e.getKey();
        ValueData val = vmap.get(key);
        if (val == null) {
          val = new ValueData(e.getValue());
          vmap.put(key,val);
        }
        else {
          V ret = val.insert(e.getValue(), depth);
          if (queue.isConnected() && (ret != null)) { // means something popped out of the queue
            HashMap<K,V> qtuple = new HashMap<K,V>(1);
            qtuple.put(key, ret);
            queue.emit(qtuple);
          }
        }
      }
    }
  };

  @InputPortFieldAnnotation(name = "query")
  public final transient DefaultInputPort<K> query = new DefaultInputPort<K>(this)
  {
    /**
     * Processes query, and emits Console tuple
     */
    @Override
    public void process(K tuple)
    {
      queryHash.put(tuple, new Object());
      emitConsoleTuple(tuple);
    }
  };

  @OutputPortFieldAnnotation(name = "queue")
  public final transient DefaultOutputPort<HashMap<K,V>> queue = new DefaultOutputPort<HashMap<K,V>>(this);

  @OutputPortFieldAnnotation(name = "console")
  public final transient DefaultOutputPort<HashMap<K,ArrayList<V>>> console = new DefaultOutputPort<HashMap<K,ArrayList<V>>>(this);
  HashMap<K,ValueData> vmap = new HashMap<K,ValueData>();
  HashMap<K,Object> queryHash = new HashMap<K,Object>();
  final int depth_default = 10;
  @Min(1)
  int depth = depth_default;


  /**
   * return depth
   * @return depth
   */
  @Min(1)
  public int getDepth()
  {
    return depth;
  }

  /**
   * sets depth
   * @param d depth is set to d
   */
  public void setDepth(int d)
  {
    depth = d;
  }

  class ValueData
  {
    int index = 0;
    ArrayList<V> list = new ArrayList<V>();

    ValueData(V o)
    {
      list.add(o);
      index++;
    }

    /**
     * Inserts Object at the tail of the queue
     *
     * @param val
     * @return Object: the Object at the top of the queue after it is full
     */
    public V insert(V val, int depth)
    {
      V ret = null;
      if (list.size() >= depth) {
        if (index >= depth) { //rollover to start
          index = 0;
        }
        ret = list.get(index);
        list.set(index, val);
        index++;
      }
      else {
        list.add(val);
        index++;
      }
      return ret;
    }

    public ArrayList<V> getList(int depth)
    {
      ArrayList<V> ret = new ArrayList<V>();
      if (list.size() >= depth) { // list is full
        int i = index;
        while (i < depth) {
          ret.add(list.get(i));
          i++;
        }
        i = 0;
        while (i < index) {
          ret.add(list.get(i));
          i++;
        }
      }
      else { // not yet fully filled up
        for (int i = 0; i < index; i++) {
          ret.add(list.get(i));
        }
      }
      return ret;
    }
  }

  /**
   * Emits tuple to console port
   * @param key
   */
  void emitConsoleTuple(K key)
  {
    ValueData val = vmap.get(key);
    ArrayList<V> list;
    HashMap<K, ArrayList<V>> tuple = new HashMap<K, ArrayList<V>>(1);
    if (val != null) {
      list = val.getList(depth);
    }
    else {
      list = new ArrayList<V>(); // If no data, send an empty ArrayList
    }
    tuple.put(key, list);
    console.emit(tuple);
  }

  /**
   * Emits all query tuples
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, Object> e: queryHash.entrySet()) {
      emitConsoleTuple(e.getKey());
    }
  }
}
