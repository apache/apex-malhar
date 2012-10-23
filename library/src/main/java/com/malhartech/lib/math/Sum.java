/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". At end of window sums all values
 * for each key and emits them on port <b>sum</b>; emits number of occurrences on port <b>count</b>; and average on port <b>average</b><p>
 * <br> Values are stored in a
 * hash<br> This node only functions in a windowed stram application<br> Compile
 * time error processing is done on configuration parameters<br> input port
 * "data" must be connected<br> output port "sum" must be connected<br>
 * "windowed" has to be true<br> Run time error processing are emitted on _error
 * port. The errors are:<br> Value is not a Number<br>
 *
 * @author amol
 */
public class Sum<K, V extends Number> extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        K key = e.getKey();
        Double val = sums.get(key);
        if (val == null) {
          sums.put(key, e.getValue().doubleValue());
        }
        else {
          val = val + e.getValue().doubleValue();
        }
        sums.put(key, val);
        MutableInteger count = counts.get(key);
        if (count == null) {
          count = new MutableInteger(0);
          counts.put(key, count);
        }
        count.value++;
      }
    }
  };

  public final transient DefaultOutputPort<HashMap<K,V>> sum = new DefaultOutputPort<HashMap<K,V>>(this);
  public final transient DefaultOutputPort<HashMap<K,Double>> average = new DefaultOutputPort<HashMap<K,Double>>(this);
  public final transient DefaultOutputPort<HashMap<K,Integer>> count = new DefaultOutputPort<HashMap<K,Integer>>(this);

  HashMap<K,Double> sums = new HashMap<K,Double>();
  HashMap<K,MutableInteger> counts = new HashMap<K,MutableInteger>();


  @Override
  public void beginWindow()
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

    HashMap<K,V> stuples = null;
    if (sum.isConnected()) {
      stuples = new HashMap<K,V>();
    }

    HashMap<K,Integer> ctuples = null;
    if (count.isConnected()) {
      ctuples = new HashMap<K,Integer>();
    }

    HashMap<K,Double> atuples = null;
    if (average.isConnected()) {
      atuples = new HashMap<K,Double>();
    }

    V sval = null;
    for (Map.Entry<K,Double> e: sums.entrySet()) {
      K key = e.getKey();
      if (sum.isConnected()) {
        if (sval instanceof Double) {
          sval = (V) e.getValue();
        }
        else if (sval instanceof Integer) {
          Integer i = e.getValue().intValue();
          sval = (V) i;
        }
        else if (sval instanceof Float) {
          Float f = e.getValue().floatValue();
          sval = (V) f;
        }
        else if (sval instanceof Long) {
          Long l = e.getValue().longValue();
          sval = (V) l;
        }
        else if (sval instanceof Short) {
          Short s = e.getValue().shortValue();
          sval = (V) s;
        }
        else {
          sval = (V) e.getValue();
        }
        stuples.put(key, sval);
      }
      if (count.isConnected()) {
        ctuples.put(key, new Integer(counts.get(e.getKey()).value));
      }
      if (average.isConnected()) {
          atuples.put(e.getKey(), new Double(e.getValue().doubleValue()/counts.get(e.getKey()).value));

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
