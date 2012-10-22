/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". Occurrences of each key is counted and at the end of window the most frequent key is emitted on output port "count"<p>
 * This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects K<br>
 * <b>count</b>: emits HashMap<K, Integer>(1); where String is the least frequent key, and Integer is the number of its occurrences in the window<br>
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
 * TBD<br>
 *
 * @author amol
 */
public class MostFrequentKey<K> extends BaseOperator
{
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      MutableInteger count = keycount.get(tuple);
      if (count == null) {
        count = new MutableInteger(0);
        keycount.put(tuple, count);
      }
      count.value++;
    }
  };
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>(this);
  HashMap<K, MutableInteger> keycount = new HashMap<K, MutableInteger>();

  @Override
  public void beginWindow()
  {
    keycount.clear();
  }

  @Override
  public void endWindow()
  {
    K key = null;
    int kval = -1;
    for (Map.Entry<K, MutableInteger> e: keycount.entrySet()) {
      if ((kval == -1) || // first key
              (e.getValue().value > kval)) {
        key = e.getKey();
        kval = e.getValue().value;
      }
      e.getValue().value = 0; // clear the positions
    }
    if ((key != null) && (kval > 0)) { // key is null if no
      HashMap<K, Integer> tuple = new HashMap<K, Integer>(1);
      tuple.put(key, new Integer(kval));
      count.emit(tuple);
    }
  }
}
