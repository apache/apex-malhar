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
 * Counts the number of times a key exists in a window; Count is emitted at end of window in a single HashMap<p>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<K,V><br>
 * <b>count</b>: Output data port, emits HashMap<HashMap<K,V>,Integer><br>
 * <b>Properties</b>: None<br>
 * <b>Compile time checks</b>: None<br>
 * <b>Run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 110 million tuples/sec. Only one tuple per unique key is emitted on end of window, so this operator is not bound by outbound throughput<br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UniqueCounter<K> extends BaseUniqueCounter<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>(this);

  /**
   * Emits one HashMap as tuple
   */
  @Override
  public void endWindow()
  {
    HashMap<K, Integer> tuple = null;
    for (Map.Entry<K, MutableInteger> e: map.entrySet()) {
      if (tuple == null) {
        tuple = new HashMap<K, Integer>();
      }
      tuple.put(e.getKey(), new Integer(e.getValue().value));
    }
    if (tuple != null) {
      count.emit(tuple);
    }
  }
}
