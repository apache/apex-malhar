/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Occurrences of all values for each key in a stream is counted and at the end of window the least frequent value is emitted
 * on output port "count" per key<p>
 * This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects Map<K, V><br>
 *
 * @author amol
 */
public abstract class AbstractBaseFrequentKeyValueMap<K, V> extends BaseKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>(this)
  {
    /**
     * Process every tuple to count occurrences of key,val pairs
     */
    @Override
    public void process(Map<K, V> tuple)
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

  /**
   * Clears the cache/hash
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    keyvals.clear();
  }

  /**
   * Override compareCount to decide most vs least
   *
   * @param val1
   * @param val2
   * @return result of compareValue to be done by sub-class
   */
  public abstract boolean compareValue(int val1, int val2);

  /**
   * override emitTuple to decide the port to emit to
   *
   * @param tuple
   */
  public abstract void emitTuple(HashMap<K, HashMap<V, Integer>> tuple);

  /**
   * Emits the result.
   */
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
        else if (compareValue(v.getValue().value, kval)) {
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
