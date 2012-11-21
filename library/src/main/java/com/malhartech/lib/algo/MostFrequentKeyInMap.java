/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.BaseFrequentKey;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Occurrences of each key is counted and at the end of window the most frequent key is emitted on output port "count"<p>
 * This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects HashMap<K,V>, V is ignored/not used<br>
 * <b>most</b>: emits HashMap<K, Integer>(1); where String is the most frequent key, and Integer is the number of its occurrences in the window<br>
 * <b>list</b>: emits ArrayList<HashMap<K,Integer>(1)>; Where the list includes all the keys are most frequent<br>
 * <br>
 * Properties:<br>
 * none<br>
 * <br>
 * Compile time checks<br>
 * none<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can process about 30 million unique (k immutable) object tuples/sec, and take in a lot more incoming tuples. The operator emits only one tuple per window
 * and hence is not bound by outbound throughput<br>
 *
 * @author amol
 */
public class MostFrequentKeyInMap<K,V> extends BaseFrequentKey<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * Calls super.processTuple(tuple) for each key in the HashMap
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        processTuple(e.getKey());
      }
    }
  };
  @OutputPortFieldAnnotation(name = "most")
  public final transient DefaultOutputPort<HashMap<K, Integer>> most = new DefaultOutputPort<HashMap<K, Integer>>(this);
  @OutputPortFieldAnnotation(name = "list")
  public final transient DefaultOutputPort<ArrayList<HashMap<K, Integer>>> list = new DefaultOutputPort<ArrayList<HashMap<K, Integer>>>(this);


  /**
   * Emits tuple on port "most"
   * @param tuple
   */
  @Override
  public void emitTuple(HashMap<K, Integer> tuple)
  {
    most.emit(tuple);
  }

  /**
   * Emits tuple on port "list"
   * @param tlist
   */
  @Override
  public void emitList(ArrayList<HashMap<K, Integer>> tlist)
  {
    list.emit(tlist);
  }

  /**
   * returns val1 < val2
   * @param val1
   * @param val2
   * @return val1 > val2
   */
  @Override
  public boolean compareCount(int val1, int val2)
  {
    return val1 > val2;
  }
}
