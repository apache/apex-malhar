/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.BaseUniqueCounter;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Count unique occurances of key,val pairs within a window, and emits one HashMap tuple per unique key,val pair<p>
 * <br>
 * This operator is same as the combination of {@link com.malhartech.lib.algo.UniqueKeyValCounter} followed by {@link com.malhartech.lib.stream.HashMapToKey}<br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<K,V><br>
 * <b>count</b>: Output data port, emits HashMap<HashMap<K,V>,Integer>(1)<br>
 * <b>Properties</b>: None<br>
 * <b>Compile time checks</b>: None<br>
 * <b>Run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 8 million tuples/sec. Only one tuple per unique key is emitted on end of window, so this operator is not bound by outbound throughput<br>
 *
 * @author Amol Kekre <amol@malhar-inc.com>
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
