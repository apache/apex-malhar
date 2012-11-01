/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". Takes the first N tuples of a particular key and emits them as they come in on output port "first".<p>
 * This module is a pass through module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects HashMap<K, V><br>
 * <b>first</b>: emits HashMap<K, V><br>
 * <br>
 * Properties:<br>
 * <b>n</b>: Number of tuples to pass through for each key. Default value of N is 1.<br>
 * <br>
 * Compile time checks<br>
 * N if specified must be an integer<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can process > 4 million unique (k,v immutable pairs) tuples/sec, and take in a lot more incoming tuples. The operator emits only N tuples per key per window
 * and hence is not bound by outbound throughput<br>
 * @author amol
 */

public class FirstN<K,V> extends BaseNOperator<K, V>
{
  @OutputPortFieldAnnotation(name="first")
  public final transient DefaultOutputPort<HashMap<K, V>> first = new DefaultOutputPort<HashMap<K, V>>(this);

  HashMap<K, MutableInteger> keycount = new HashMap<K, MutableInteger>();

  /**
   * Inserts tuples into the queue
   * @param tuple to insert in the queue
   */
  @Override
  public void processTuple(HashMap<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      MutableInteger count = keycount.get(e.getKey());
      if (count == null) {
        count = new MutableInteger(0);
        keycount.put(e.getKey(), count);
      }
      count.value++;
      if (count.value <= n) {
        first.emit(cloneTuple(e.getKey(), e.getValue()));
      }
    }
  }

  /**
   * Clears the cache to start anew in a new window
   */
  @Override
  public void beginWindow(long windowId)
  {
    keycount.clear();
  }
}
