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
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Occurrences of all values for each key in a stream is counted and at the end of window the least frequent value is emitted
 * on output port "count" per key<p>
 * This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<K, V><br>
 * @author amol
 */
public abstract class BaseFrequentKeyValue<K, V> extends BaseKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        HashMap<V, MutableInteger> vals = keyvals.get(e.getKey());
        if (vals == null) {
          vals = new HashMap<V, MutableInteger>(4);
          keyvals.put(cloneKey(e.getKey()), vals);
        }
        MutableInteger count = vals.get(e.getValue());
        if (count == null) {
          count = new MutableInteger(0);
          vals.put(cloneValue(e.getValue()), count);
        }
        count.value++;
      }
    }
  };
  HashMap<K, HashMap<V, MutableInteger>> keyvals = new HashMap<K, HashMap<V, MutableInteger>>();

  @Override
  public void beginWindow(long windowId)
  {
    keyvals.clear();
  }

  public abstract boolean compareValue(int val1, int val2);
  public abstract void emitTuple(HashMap<K, HashMap<V, Integer>> tuple);


  @Override
  public void endWindow()
  {
    HashMap<V, Object> vmap = new HashMap<V, Object>();
    for (Map.Entry<K, HashMap<V, MutableInteger>> e: keyvals.entrySet()) {
      V val = null;
      int kval = -1;
      vmap.clear();
      HashMap<V, MutableInteger> vals = e.getValue();
      for (Map.Entry<V, MutableInteger> v: vals.entrySet()) {
        if (kval == -1) {
          val = v.getKey();
          kval = v.getValue().value;
          vmap.put(val, null);
        }
        else if (compareValue(v.getValue().value,kval)) {
          val = v.getKey();
          kval = v.getValue().value;
          vmap.clear();
          vmap.put(val, null);
        }
        else if (v.getValue().value == kval) {
          vmap.put(v.getKey(), null);
        }
      }
      if ((val != null) && (kval > 0)) { // key is null if no
        HashMap<K, HashMap<V, Integer>> tuple = new HashMap<K, HashMap<V, Integer>>(1);
        HashMap<V, Integer> valpair = new HashMap<V, Integer>();
        for (Map.Entry<V, Object> v: vmap.entrySet()) {
          valpair.put(v.getKey(), new Integer(kval));
        }
        tuple.put(e.getKey(), valpair);
        emitTuple(tuple);
      }
    }
  }
}
