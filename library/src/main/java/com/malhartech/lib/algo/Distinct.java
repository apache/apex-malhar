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
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Computes and emits distinct key,val pairs (i.e drops duplicates) at end of window<p>
 * <br>
 * This module is same as a "FirstOf" operation on any key, val pair
 * Even though this module produces continuous tuples, at end of window all data is flushed. Thus the data set is windowed
 * and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<K,V>
 * <b>distinct</b>: Output data port, emits HashMap<K,V>(1)
 * <b>Properties</b>:
 * None
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * None
 * <br>
 * Run time checks are:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can emit >4 million unique (k,v pairs) tuples/sec, and take in a lot more incoming tuples. The performance is directly proportional to unique key,val pairs emitted<br>
 * <br>
 * @author amol<br>
 *
 */

public class Distinct<K,V> extends BaseOperator
{
  @InputPortFieldAnnotation(name="data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * Process HashMap<K,V> tuple on input port data, and emits if match not found. Updates the cache
     * with new key,val pair
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        HashMap<V, Object> vals = mapkeyval.get(e.getKey());
        if ((vals == null) || !vals.containsKey(e.getValue())) {
          HashMap<K,V> otuple = new HashMap<K,V>(1);
          otuple.put(e.getKey(), e.getValue());
          distinct.emit(otuple);
          if (vals == null) {
            vals = new HashMap<V, Object>();
            mapkeyval.put(e.getKey(), vals);
          }
          vals.put(e.getValue(), null);
        }
      }
    }
  };

  @OutputPortFieldAnnotation(name="distinct")
  public final transient DefaultOutputPort<HashMap<K,V>> distinct = new DefaultOutputPort<HashMap<K,V>>(this);
  HashMap<K, HashMap<V, Object>> mapkeyval = new HashMap<K, HashMap<V, Object>>();

  /**
   * Clears the cache/hash
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    mapkeyval.clear();
  }
}
