/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". Occurrences of all values for each key is counted and at the end of window the least frequent value is emitted
 * on output port "count" per key<p>
 * This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<K, V><br>
 * <b>count</b>: Output port, emits HashMap<K, HashMap<V, Integer>(1)>(1), where first String is the key, the second String is the value, and Integer is the count of occurrence<br>
 * <br>
 * Properties:<br>
 * none<br>
 * <br>
 * Compile time checks<br>
 * none<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * TBD<br>
 *
 * @author amol
 */
public class LeastFrequentKeyValue<K,V> extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    @Override
    public void process(HashMap<K,V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        HashMap<V, MutableInteger> vals = keyvals.get(e.getKey());
        if (vals == null) {
          vals = new HashMap<V, MutableInteger>(4);
        }
        MutableInteger count = vals.get(e.getValue());
        if (count == null) {
          count = new MutableInteger(0);
          vals.put(e.getValue(), count);
        }
        count.value++;
      }
    }
  };
  public final transient DefaultOutputPort<HashMap<K, HashMap<V, Integer>>> count = new DefaultOutputPort<HashMap<K, HashMap<V, Integer>>>(this);
  HashMap<K, HashMap<V, MutableInteger>> keyvals = new HashMap<K, HashMap<V, MutableInteger>>();

  @Override
  public void beginWindow()
  {
    keyvals.clear();
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<K, HashMap<V, MutableInteger>> e: keyvals.entrySet()) {
      V val = null;
      int kval = -1;
      HashMap<V, MutableInteger> vals = e.getValue();
      for (Map.Entry<V, MutableInteger> v: vals.entrySet()) {
        if ((kval == -1) || // first key
                (v.getValue().value < kval)) {
          val = v.getKey();
          kval = v.getValue().value;
        }
      }
      if ((val != null) && (kval > 0)) { // key is null if no
        HashMap<K, HashMap<V, Integer>> tuple = new HashMap<K, HashMap<V, Integer>>(1);
        HashMap<V, Integer> valpair = new HashMap<V, Integer>(1);
        valpair.put(val, new Integer(kval));
        tuple.put(e.getKey(), valpair);
        count.emit(tuple);
      }
    }
  }
}
