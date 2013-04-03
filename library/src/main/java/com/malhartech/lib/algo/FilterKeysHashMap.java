/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyOperator;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;


/**
 *
 * Filters the incoming stream based of keys specified by property "keys". If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted<p>
 * Operator assumes that the key, val pairs are immutable objects. If this operator has to be used for mutable objects,
 * override "cloneKey()" to make copy of K, and "cloneValue()" to make copy of V.<br>
 * This is a pass through node.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Expects Map&lt;K, HashMap&lt;K,V&gt;&gt. Filters are applied only on keys of second hash map.<br>
 * <b>filter</b>: Emits HashMap&lt;K, HashMap&lt;K,V&gt;&gt.<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>keys</b>: The keys to pass through, rest are filtered/dropped. A comma separated list of keys.<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * keys cannot be empty<br>
 * <b>Specific run time checks</b>: None <br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for FilterKeys&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 15 Million K,V pairs/s (4 million out-bound emits/s)</b></td><td>Emits all K,V pairs in a tuple such that K is in the filter list
 * (or not in the list if inverse is set to true)</td><td>In-bound throughput and number of matching K are the main determinant of performance.
 * Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); inverse=false; keys="a,b,h"</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for FilterKeys&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>filter</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td>{a=2}<br>{b=20}</td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td>{a=-1}</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5}</td><td>{b=5}</td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=-5}</td><td>{a=5}<br>{b=-5}</td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=1000,b=-5}</td><td>{a=3}<br>{h=20}</td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=1,d=5,d=55}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=1,a=3,e=2}</td><td>{a=3}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Locknath Shil <locknath@malhar-inc.com> <br>
 * <br>
 *
 */
public class FilterKeysHashMap<K, V> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, HashMap<K, V>>> data = new DefaultInputPort<Map<K, HashMap<K, V>>>(this)
  {
    /**
     * Processes incoming tuples one key,val at a time. Emits if at least one key makes the cut.
     * By setting inverse as true, match is changed to un-matched.
     */
    @Override
    public void process(Map<K, HashMap<K, V>> tuple)
    {
      HashMap<K, HashMap<K, V>> dtuple = null;
      for (Map.Entry<K, HashMap<K, V>> e: tuple.entrySet()) {
        HashMap<K, V> dtuple2 = null;
        for (Map.Entry<K, V> e2: e.getValue().entrySet()) {
          boolean contains = keys.containsKey(e2.getKey());
          if ((contains && !inverse) || (!contains && inverse)) {
            if (dtuple2 == null) {
              dtuple2 = new HashMap<K, V>(4); // usually the filter keys are very few, so 4 is just fine
            }
            dtuple2.put(cloneKey(e2.getKey()), cloneValue(e2.getValue()));
          }
        }
        if (dtuple == null && dtuple2 != null){
          dtuple = new HashMap<K, HashMap<K, V>>();
        }
        if (dtuple != null && dtuple2 != null){
          dtuple.put(cloneKey(e.getKey()), dtuple2);
        }
      }
      if (dtuple != null) {
        filter.emit(dtuple);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "filter")
  public final transient DefaultOutputPort<HashMap<K, HashMap<K, V>>> filter = new DefaultOutputPort<HashMap<K, HashMap<K, V>>>(this);
  @NotNull()
  HashMap<K, V> keys = new HashMap<K, V>();
  boolean inverse = false;

  /**
   * getter function for parameter inverse
   *
   * @return inverse
   */
  public boolean getInverse()
  {
    return inverse;
  }

  /**
   * True means match; False means unmatched
   *
   * @param val
   */
  public void setInverse(boolean val)
  {
    inverse = val;
  }

  /**
   * Adds a key to the filter list
   *
   * @param str
   */
  public void setKey(K str)
  {
    keys.put(str, null);
  }

  /**
   * Adds the list of keys to the filter list
   *
   * @param list
   */
  public void setKeys(K[] list)
  {
    if (list != null) {
      for (K e: list) {
        keys.put(e, null);
      }
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
   *
   * @param v value to be cloned
   * @return returns v as is (assumes immutable object)
   */
  public V cloneValue(V v)
  {
    return v;
  }
}
