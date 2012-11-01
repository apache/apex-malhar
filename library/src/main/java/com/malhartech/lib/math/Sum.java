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
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

/**
 *
 * Takes in one stream via input port "data". At end of window sums all values
 * for each key and emits them on port <b>sum</b>; emits number of occurrences on port <b>count</b>; and average on port <b>average</b><p>
 * <br>
 * Is an end of window operator<br>
 * <b>Ports</b>:
 * <b>data</b>: expects HashMap<K,V extends Number><br>
 * <b>sum</b>: emits HashMap<K,V><br>
 * <b>count</b>: emits HashMap<K,Integer></b><br>
 * <b>average</b>: emits HashMap<K,V></b><br>
 * Compile time checks<br>
 * None<br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Over 6 million tuples/sec as all tuples are absorbed, and only one goes out at end of window<br>
 * <br>
 *
 * @author amol
 */
public class Sum<K, V extends Number> extends BaseNumberOperator<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        K key = e.getKey();
        if (sum.isConnected()) {
          Double val = sums.get(key);
          if (val == null) {
            val = e.getValue().doubleValue();
          }
          else {
            val = val + e.getValue().doubleValue();
          }
          sums.put(key, val);
        }
        if (count.isConnected() || average.isConnected()) {
          MutableInteger count = counts.get(key);
          if (count == null) {
            count = new MutableInteger(0);
            counts.put(key, count);
          }
          count.value++;
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "sum")
  public final transient DefaultOutputPort<HashMap<K, V>> sum = new DefaultOutputPort<HashMap<K, V>>(this);
  @OutputPortFieldAnnotation(name = "average")
  public final transient DefaultOutputPort<HashMap<K, V>> average = new DefaultOutputPort<HashMap<K, V>>(this);
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>(this);
  HashMap<K, Double> sums = new HashMap<K, Double>();
  HashMap<K, MutableInteger> counts = new HashMap<K, MutableInteger>();

  @Override
  public void beginWindow(long windowId)
  {
    sums.clear();
    counts.clear();
  }

  /**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {

    // Should allow users to send each key as a separate tuple to load balance
    // This is an aggregate node, so load balancing would most likely not be needed

    HashMap<K, V> stuples = null;
    if (sum.isConnected()) {
      stuples = new HashMap<K, V>();
    }

    HashMap<K, Integer> ctuples = null;
    if (count.isConnected()) {
      ctuples = new HashMap<K, Integer>();
    }

    HashMap<K, V> atuples = null;
    if (average.isConnected()) {
      atuples = new HashMap<K, V>();
    }

    if (sum.isConnected()) {
      for (Map.Entry<K, Double> e: sums.entrySet()) {
        K key = e.getKey();
        if (sum.isConnected()) {
          stuples.put(key, getValue(e.getValue()));
        }
        if (count.isConnected()) {
          ctuples.put(key, new Integer(counts.get(e.getKey()).value));
        }
        if (average.isConnected()) {
          atuples.put(e.getKey(), getValue(e.getValue().doubleValue() / counts.get(e.getKey()).value));
        }
      }
    }
    else if (count.isConnected()) { // sum is not connected, only counts is connected
      for (Map.Entry<K, MutableInteger> e: counts.entrySet()) {
        ctuples.put(e.getKey(), new Integer(e.getValue().value));
      }
    }

    if ((stuples != null) && !stuples.isEmpty()) {
      sum.emit(stuples);
    }
    if ((ctuples != null) && !ctuples.isEmpty()) {
      count.emit(ctuples);
    }
    if ((atuples != null) && !atuples.isEmpty()) {
      average.emit(atuples);
    }
  }
}
