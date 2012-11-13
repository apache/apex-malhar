/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.BaseKeyOperator;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Filters the incoming stream based of keys specified by property "keys". If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted<p>
 * Operator assumes that the key, val pairs are immutable objects. If this operator has to be used for mutable objects, override "cloneKey" to make copy of K, and
 * "cloneValue" to make copy of V.
 * <br>
 * This is a pass through node. It takes in HashMap<String, V> and outputs HashMap<String, V><br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, V>
 * <b>filter</b>: Output data port, emits HashMap<String, V>
 * <b>Properties</b>:
 * <b>keys</b>: The keys to pass through, rest are filtered/dropped. A comma separated list of keys<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <br>
 * Run time checks are:<br>
 * None
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can emit about 8 million unique (k,v immutable pairs) tuples/sec, and take in a lot more incoming tuples. The performance is directly proportional to key,val pairs emitted<br>
 * <br>
 * @author amol<br>
 *
 */

public class FilterKeys<K,V> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name="data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Processes incoming tuples one key,val at a time. Emits if at least one key makes the cut
     * By setting inverse as true, match is changed to un-matched
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      HashMap<K, V> dtuple = null;
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        boolean contains = keys.containsKey(e.getKey());
        if ((contains && !inverse) || (!contains && inverse)) {
          if (dtuple == null) {
            dtuple = new HashMap<K, V>(4); // usually the filter keys are very few, so 4 is just fine
          }
          dtuple.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
        }
      }
      if (dtuple != null) {
        filter.emit(dtuple);
      }
    }
  };

  @OutputPortFieldAnnotation(name="filter")
  public final transient DefaultOutputPort<HashMap<K, V>> filter = new DefaultOutputPort<HashMap<K, V>>(this);

  HashMap<K, V> keys = new HashMap<K, V>();
  boolean inverse = false;

  /**
   * True means match; False means unmatched
   * @param val
   */
  public void setInverse(boolean val) {
    inverse = val;
  }

  /**
   * Adds a key to the filter list
   * @param str
   */
  public void setKey(K str) {
      keys.put(str, null);
  }

  /**
   * Adds the list of keys to the filter list
   * @param list
   */
  public void setKeys(ArrayList<K> list)
  {
    for (K e : list) {
      keys.put(e,null);
    }
  }

  /*
   * Clears the filter list
   */
  public void clearKeys()
  {
    keys.clear();
  }

  /**
   * Clones V object. By default assumes immutable object (i.e. a copy is not made). If object is mutable, override this method
   * @param v
   * @return
   */
  public V cloneValue(V v)
  {
    return v;
  }
}
