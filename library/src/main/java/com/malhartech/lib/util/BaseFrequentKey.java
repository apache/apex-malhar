/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.lib.util.BaseKeyOperator;
import com.malhartech.lib.util.MutableInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Occurrences of each key is counted in the input stream, and at the end of window the winning frequent key is emitted on output port "count"<p>
 * This module is an end of window module<br>
 *
 * @author amol
 */
public abstract class BaseFrequentKey<K> extends BaseKeyOperator<K>
{
  /**
   * Counts frequency of a key
   * @param tuple
   */
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

  /**
   * override emitTuple to decide the port to emit to
   * @param tuple
   */
  abstract public void emitTuple(HashMap<K,Integer> tuple);
  /**
   * Overide emitList to specify the emit schema
   * @param tlist
   */
  abstract public void emitList(ArrayList<HashMap<K, Integer>> tlist);
  /**
   * Override compareCount to decide most vs least
   * @param val1
   * @param val2
   * @return result of compareCount to be done by subclass
   */
  abstract public boolean compareCount(int val1, int val2);

  /**
   * Clears the cache/hash
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    keycount.clear();
  }

  /**
   * Emits the result.
   */
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
