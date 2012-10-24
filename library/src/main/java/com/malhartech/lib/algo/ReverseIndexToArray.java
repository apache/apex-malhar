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
import java.util.Map;

/**
 *
 * Takes a stream via input port "data" and emits the reverse index on output port index on end of window<p>
 * <br>
 * Takes in HashMap<Object, Object> and emits HashMap<Object, Object>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<Object, Object>
 * <b>index</b>: Output data port, emits HashMap<Object, ArrayList<Object>>
 * <b>Properties</b>:
 *
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <br>
 * Run time checks are:<br>
 *
 *
 * @author amol<br>
 *
 */

public class ReverseIndexToArray<K, V> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        ArrayList<K> list = map.get(e.getValue());
        if (list == null) {
          list = new ArrayList<K>(4);
        }
        list.add(e.getKey());
      }
    }
  };

  @OutputPortFieldAnnotation(name = "index")
  public final transient DefaultOutputPort<HashMap<V, ArrayList<K>>> index = new DefaultOutputPort<HashMap<V, ArrayList<K>>>(this);
  HashMap<V, ArrayList<K>> map = new HashMap<V, ArrayList<K>>();

  @Override
  public void beginWindow()
  {
    map.clear();
  }

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
