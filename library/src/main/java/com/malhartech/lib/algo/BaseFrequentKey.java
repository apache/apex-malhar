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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". Occurrences of each key is counted and at the end of window the winning frequent key is emitted on output port "count"<p>
 * This module is an end of window module<br>
 *
 * @author amol
 */
public abstract class BaseFrequentKey<K> extends BaseKeyOperator<K>
{
  public void processTuple(K tuple)
  {
    MutableInteger count = keycount.get(tuple);
    if (count == null) {
      count = new MutableInteger(0);
      keycount.put(cloneKey(tuple), count);
    }
    count.value++;
  }
  HashMap<K, MutableInteger> keycount = new HashMap<K, MutableInteger>();

  abstract public void emitTuple(HashMap<K,Integer> tuple);
  abstract public void emitList(ArrayList<HashMap<K, Integer>> tlist);
  abstract public boolean compareCount(int val1, int val2);

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
    HashMap<K, Object> map = new HashMap<K, Object>();
    for (Map.Entry<K, MutableInteger> e: keycount.entrySet()) {
      if ((kval == -1)) {
        key = e.getKey();
        kval = e.getValue().value;
        map.put(key, null);
      }
      else if (compareCount(e.getValue().value, kval)) {
        key = e.getKey();
        kval = e.getValue().value;
        map.clear();
        map.put(key, null);
      }
      else if (e.getValue().value == kval) {
        map.put(e.getKey(), null);
      }
    }
    HashMap<K, Integer> tuple;
    if ((key != null) && (kval > 0)) { // key is null if no
      tuple = new HashMap<K, Integer>(1);
      tuple.put(key, new Integer(kval));
      emitTuple(tuple);
      ArrayList<HashMap<K, Integer>> elist = new ArrayList<HashMap<K, Integer>>();
      for (Map.Entry<K, Object> e: map.entrySet()) {
        tuple = new HashMap<K, Integer>(1);
        tuple.put(e.getKey(), kval);
        elist.add(tuple);
      }
      emitList(elist);
    }
  }
}
