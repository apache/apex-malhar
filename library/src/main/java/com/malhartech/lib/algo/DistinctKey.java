package com.malhartech.lib.algo;

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyOperator;
import java.util.HashMap;

/**
 *
 * Computes and emits distinct tuples of type K (i.e drops duplicates) at end of window<p>
 * <br>
 * This module is same as a "FirstOf" operation on any key, val pair
 * Even though this module produces continuous tuples, at end of window all data is flushed. Thus the data set is windowed
 * and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects K
 * <b>distinct</b>: Output data port, emits K
 * <b>Properties</b>: None<br>
 * <b>Compile time checks</b>: None<br>
 * <b>Run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can emit > 10 million unique tuples/sec, and take in a lot more incoming tuples. The performance is directly proportional to
 * unique tuples emitted<br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class DistinctKey<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Process HashMap<K,V> tuple on input port data, and emits if match not found. Updates the cache
     * with new key,val pair
     */
    @Override
    public void process(K tuple)
    {
      if (!map.containsKey(tuple)) {
        distinct.emit(cloneKey(tuple));
        map.put(cloneKey(tuple), null);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "distinct")
  public final transient DefaultOutputPort<K> distinct = new DefaultOutputPort<K>(this);
  HashMap<K, Object> map = new HashMap<K, Object>();

  /**
   * Clears the cache/hash
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    map.clear();
  }
}
