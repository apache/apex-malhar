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
 * Counts the number of times a key exists in a window. Count is emitted at end of window<p>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 110 million tuples/sec. Only one tuple per unique key is emitted on end of window, so this operator is not bound by outbound throughput<br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UniqueCounterKey<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      MutableInteger i = map.get(tuple);
      if (i == null) {
        map.put(cloneKey(tuple), new MutableInteger(1));
      }
      else {
        i.value++;
      }
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>(this);
  /**
   * Bucket counting mechanism.
   * Since we clear the bucket at the beginning of the window, we make this object transient.
   */
  transient HashMap<K, MutableInteger> map = new HashMap<K, MutableInteger>();

  @Override
  public void beginWindow(long windowId)
  {
    map.clear();
  }

  @Override
  public void endWindow()
  {
    // emitting one key at a time helps in load balancing
    // If MutableInteger is supported, then there is no need to create a new hash
    // just emit(map) would suffice
    for (Map.Entry<K, MutableInteger> e: map.entrySet()) {
      HashMap<K, Integer> tuple = new HashMap<K, Integer>(1);
      tuple.put(e.getKey(), new Integer(e.getValue().value));
      count.emit(tuple);
    }
  }
}
