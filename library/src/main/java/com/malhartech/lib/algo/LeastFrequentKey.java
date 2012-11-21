/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.AbstractBaseFrequentKey;
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
 *  Occurrences of each key is counted and at the end of window the least frequent key is emitted on output port "count"<p>
 * This module is an end of window module<br>
 * In case of a tie
 * <br>
 * Ports:<br>
 * <b>data</b>: expects K<br>
 * <b>least</b>: emits HashMap<K,Integer>(1); where String is the least frequent key, and Integer is the number of its occurrences in the window. In case of tie
 * any of th least key would be emitted<br>
 * <b>list</b>: emits ArrayList<HashMap<K,Integer>(1)>; Where the list includes all the keys are least frequent<br>
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
 * Operator can process > 60 million unique (k immutable object tuples/sec, and take in a lot more incoming tuples. The operator emits only one tuple per window
 * and hence is not bound by outbound throughput<br>
 * <br>
 * @author amol
 */
public class LeastFrequentKey<K> extends AbstractBaseFrequentKey<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Calls super.processTuple(tuple)
     */
    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }
  };

  @OutputPortFieldAnnotation(name = "least", optional=true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> least = new DefaultOutputPort<HashMap<K, Integer>>(this);
  @OutputPortFieldAnnotation(name = "list", optional=true)
  public final transient DefaultOutputPort<ArrayList<HashMap<K, Integer>>> list = new DefaultOutputPort<ArrayList<HashMap<K, Integer>>>(this);

  /**
   * Emits tuple on port "least"
   * @param tuple
   */
  @Override
  public void emitTuple(HashMap<K, Integer> tuple)
  {
    least.emit(tuple);
  }

  /**
   * returns val1 < val2
   * @param val1
   * @param val2
   * @return val1 < val2
   */
  @Override
    public boolean compareCount(int val1, int val2)
  {
    return val1 < val2;
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
}
