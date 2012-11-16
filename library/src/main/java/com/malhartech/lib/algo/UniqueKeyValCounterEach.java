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
 * Count unique occurances of keys within a window<p>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 110 million tuples/sec. Only one tuple per unique key is emitted on end of window, so this operator is not bound by outbound throughput<br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UniqueKeyValCounterEach<K,V> extends BaseUniqueCounter<HashMap<K,V>>
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
      HashMap<K,V> keyvalpair;
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        keyvalpair = new HashMap<K,V>(1);
        keyvalpair.put(e.getKey(), e.getValue());
        processTuple(keyvalpair);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<HashMap<K,V>, Integer>> count = new DefaultOutputPort<HashMap<HashMap<K,V>, Integer>>(this);

  /**
   * Emits one HashMap as tuple
   */
  @Override
  public void endWindow()
  {
    HashMap<HashMap<K,V>, Integer> tuple;
    for (Map.Entry<HashMap<K,V>, MutableInteger> e: map.entrySet()) {
      tuple = new HashMap<HashMap<K,V>,Integer>(1);
      tuple.put(e.getKey(), new Integer(e.getValue().value));
      count.emit(tuple);
    }
  }
}
