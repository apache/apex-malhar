/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableInt;

/**
 *
 * Occurrences of each key is counted in the input stream, and at the end of window the winning frequent key is emitted on output port "count"<p>
 * This module is an end of window module<br>
 *
 * @author amol
 */
public abstract class AbstractBaseFrequentKey<K> extends BaseKeyOperator<K>
{
  /**
   * Counts frequency of a key
   * @param tuple
   */
  public void processTuple(K tuple)
  {
    MutableInt count = keycount.get(tuple);
    if (count == null) {
      count = new MutableInt(0);
      keycount.put(cloneKey(tuple), count);
    }
    count.increment();
  }
  protected HashMap<K, MutableInt> keycount = new HashMap<K, MutableInt>();

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
   * Emits the result.
   */
  @Override
  public void endWindow()
  {
    K key = null;
    int kval = -1;
    HashMap<K, Object> map = new HashMap<K, Object>();
    for (Map.Entry<K, MutableInt> e: keycount.entrySet()) {
      if ((kval == -1)) {
        key = e.getKey();
        kval = e.getValue().intValue();
        map.put(key, null);
      }
      else if (compareCount(e.getValue().intValue(), kval)) {
        key = e.getKey();
        kval = e.getValue().intValue();
        map.clear();
        map.put(key, null);
      }
      else if (e.getValue().intValue() == kval) {
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
    keycount.clear();
  }
}
