/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Count unique occurances of key,val pairs within a window, and emits one HashMap tuple<p>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<K,V><br>
 * <b>count</b>: Output data port, emits HashMap<HashMap<K,V>,Integer>(1)<br>
 * <b>Properties</b>: None<br>
 * <b>Compile time checks</b>: None<br>
 * <b>Run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 8 million tuples/sec. Only one tuple per unique key,val pair is emitted on end of window, so this operator is not bound by outbound throughput<br>
 *
 * @author Amol Kekre <amol@malhar-inc.com>
 */
public class UniqueKeyValCounter<K,V> extends BaseUniqueCounter<HashMap<K,V>>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      HashMap<K,V> keyvalpair = new HashMap<K,V>(1);
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        keyvalpair.clear();
        keyvalpair.put(e.getKey(), e.getValue());
        processTuple(keyvalpair);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<HashMap<K,V>, Integer>> count = new DefaultOutputPort<HashMap<HashMap<K,V>, Integer>>(this);

  /*
   * Cloning to allow reuse of same hash object for processing each key,val pair
   */
  @Override
  public HashMap<K, V> cloneKey(HashMap<K, V> key)
  {
    HashMap<K, V> ret = new HashMap<K, V>(1);
    for (Map.Entry<K, V> e: key.entrySet()) {
      ret.put(e.getKey(), e.getValue());
    }
    return ret;
  }

  /**
   * Emits one HashMap as tuple
   */
  @Override
  public void endWindow()
  {
    HashMap<HashMap<K,V>, Integer> tuple = null;
    for (Map.Entry<HashMap<K,V>, MutableInteger> e: map.entrySet()) {
      if (tuple == null) {
        tuple = new HashMap<HashMap<K,V>, Integer>();
      }
      tuple.put(e.getKey(), new Integer(e.getValue().value));
    }
    if (tuple != null) {
      count.emit(tuple);
    }
  }
}
