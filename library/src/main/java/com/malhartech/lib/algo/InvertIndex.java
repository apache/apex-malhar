/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index" at the end of the window<p>
 * This is an end of window operator<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>:
 * <b>data</b>: expects <K, V><br>
 * <b>index</b>: emits <V, ArrayList<K>><br>
 * <b>Properties</b>:
 * None<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * None<br>
 * <br>
 * Run time checks are:<br>
 * None<br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class InvertIndex<K, V> extends BaseOperator
{

  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Reverse indexes a HashMap<K, ArrayList<V>> tuple
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
          insert(e.getValue(), e.getKey());
      }
    }
  };
  @OutputPortFieldAnnotation(name = "index")
  public final transient DefaultOutputPort<HashMap<V, ArrayList<K>>> index = new DefaultOutputPort<HashMap<V, ArrayList<K>>>(this);
  HashMap<V, ArrayList<K>> map = new HashMap<V, ArrayList<K>>();

  /**
   *
   * Returns the ArrayList stored for a key
   *
   * @param key
   * @return ArrayList
   */
  void insert(V val, K key)
  {
    ArrayList<K> list = map.get(val);
    if (list == null) {
      list = new ArrayList<K>(4);
      map.put(val, list);
    }
    list.add(key);
  }

  @Override
  public void beginWindow()
  {
    map.clear();
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<V, ArrayList<K>> e: map.entrySet()) {
      HashMap<V, ArrayList<K>> tuple = new HashMap<V, ArrayList<K>>(1);
      tuple.put(e.getKey(), e.getValue());
      index.emit(tuple);
    }
  }
}
