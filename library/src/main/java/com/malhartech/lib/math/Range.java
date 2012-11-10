/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

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
 * Emits the range for each key at the end of window<p>
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
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Process each key and computes new high and low
     */
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
        if (val == null) {
          low.put(key, e.getValue());
        }
        else if (val.doubleValue() > eval) {
          low.put(key, e.getValue());
        }

        val = high.get(key);
        if (val == null) {
          high.put(key, e.getValue());
        }
        else if (val.doubleValue() < eval) {
          high.put(key, e.getValue());
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "range")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> range = new DefaultOutputPort<HashMap<K, ArrayList<V>>>(this);
  HashMap<K, V> high = new HashMap<K, V>();
  HashMap<K, V> low = new HashMap<K, V>();

  /**
   * Clears the cache/hash
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    high.clear();
    low.clear();
  }

  /**
   * Emits range for each key. If no data is received, no emit is done
   */
  @Override
  public void endWindow()
  {
    HashMap<K, ArrayList<V>> tuples = new HashMap<K, ArrayList<V>>(1);
    for (Map.Entry<K, V> e: high.entrySet()) {
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
