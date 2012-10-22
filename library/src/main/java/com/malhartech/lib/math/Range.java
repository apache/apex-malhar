/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". At end of window sends range of all values
 * for each key and emits them on port "range"<p>
 * <br>
 * <b>Ports</b>
 * <b>data</b> expects HashMap<K,V extends Number><br>
 * <b>range</b> emits HashMap<K,ArrayList<V>>, each key has two entries, first Max and next Min<br>
* <b>Compile time check</b>
* None<br>
* <br>
* <b>Run time checks</b><br>
* None<br>
* <br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Integer: 8 million tuples/s<br>
 * Double: 8 million tuples/s<br>
 * Long: 8 million tuples/s<br>
 * Short: 8 million tuples/s<br>
 * Float: 8 million tupels/s<br>
 *
 * @author amol
 */


public class Range<K, V extends Number> extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        K key = e.getKey();
        if (e.getValue() == null) {
          continue;
        }
        double eval = e.getValue().doubleValue();
        V val = low.get(key);
        if (val.doubleValue() > eval) {
          low.put(key, e.getValue());
        }
        val = high.get(key);
        if (val.doubleValue() < eval) {
          high.put(key, e.getValue());
        }
      }
    }
  };
  public final transient DefaultOutputPort<HashMap<K,ArrayList<V>>> range = new DefaultOutputPort<HashMap<K,ArrayList<V>>>(this);
  HashMap<K,V> high = new HashMap<K,V>();
  HashMap<K,V> low = new HashMap<K,V>();

  @Override
  public void beginWindow()
  {
    high.clear();
    low.clear();
  }

/**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {
    HashMap<K,ArrayList<V>> tuples = new HashMap<K,ArrayList<V>>(1);
    for (Map.Entry<K,V> e: high.entrySet()) {
      ArrayList alist = new ArrayList();
      alist.add(e.getValue());
      alist.add(low.get(e.getKey())); // cannot be null
      tuples.put(e.getKey(), alist);
    }
    if (!tuples.isEmpty()) {
      range.emit(tuples);
    }
  }
}

