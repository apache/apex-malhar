/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes a stream via input port "data" and emits the reverse index on output port index<p>
 * <br>
 * Takes in HashMap<Object, Object> and emits HashMap<Object, Object>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: expects HashMap<K,V><br>
 * <b>index</b>:  emits HashMap<V,K><br>
 * <b>index_list</b>: emits HashMap<V,ArrayList<K>><br>
 * <b>Properties</b>:
 * None
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <br>
 * Run time checks are:<br>
 * None
 * @author amol<br>
 *
 */
public class ReverseIndex<K, V> extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        if (index.isConnected()) {
          HashMap<V, K> otuple = new HashMap<V, K>(1);
          otuple.put(e.getValue(), e.getKey());
          index.emit(otuple);
        }
        if (index_list.isConnected()) {
          ArrayList<K> list = map.get(e.getValue());
          if (list == null) {
            list = new ArrayList<K>(4);
          }
          list.add(e.getKey());
        }
      }
    }
  };
  public final transient DefaultOutputPort<HashMap<V, K>> index = new DefaultOutputPort<HashMap<V, K>>(this);
  public final transient DefaultOutputPort<HashMap<V, ArrayList<K>>> index_list = new DefaultOutputPort<HashMap<V, ArrayList<K>>>(this);
  HashMap<V, ArrayList<K>> map = new HashMap<V, ArrayList<K>>();

  @Override
  public void beginWindow()
  {
    map.clear();
  }

  @Override
  public void endWindow()
  {
    if (index_list.isConnected()) {
      for (Map.Entry<V, ArrayList<K>> e: map.entrySet()) {
        HashMap<V, ArrayList<K>> tuple = new HashMap<V, ArrayList<K>>(1);
        tuple.put(e.getKey(), e.getValue());
        index_list.emit(tuple);
      }
    }
  }
}
