/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". At end of window sends minimum of all values
 * for each key and emits them on port "min"<p>
 * <br>
 * <b>Ports</b>:
 * <b>data</b> expects HashMap<K,V extends Number><br>
 * <b>min</b> emits HashMap<K,V>, one entry per key<br>
 * <br>
 * <b>Compile time checks</b>:
 * None<br>
 * <b>Run time checks</b>:
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 *<br>
 * @author amol
 */
public class Min<K, V extends Number> extends BaseOperator
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
        V val = low.get(e.getKey());
        if (val.doubleValue() > e.getValue().doubleValue()) {
          low.put(key, e.getValue());
        }
      }
    }
  };
  public final transient DefaultOutputPort<HashMap<K,V>> min = new DefaultOutputPort<HashMap<K,V>>(this);
  HashMap<K,V> low = new HashMap<K,V>();

  @Override
  public void beginWindow()
  {
    low.clear();
  }

  /**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {
    if (!low.isEmpty()) {
      min.emit(low);
    }
  }
}
