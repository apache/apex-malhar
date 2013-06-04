package com.malhartech.lib.algo;

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */


import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyOperator;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

/**
 *
 * Filters the incoming stream based of specified key,val pairs, and emits those that match the filter. If
 * property "inverse" is set to "true", then all key,val pairs except those specified by in keyvals parameter are emitted<p>
 * Operator assumes that the key, val pairs are immutable objects. If this operator has to be used for mutable objects,
 * override "cloneKey()" to make copy of K, and "cloneValue()" to make copy of V.<br>
 * This is a pass through node<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>filter</b>: emits HashMap&lt;K,V&gt;(1)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>keyvals</b>: The keyvals is key,val pairs to pass through, rest are filtered/dropped.<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * keyvals cannot be empty<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for FilterKeyVals&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 8 Million K,V pairs/s (4 million out-bound emits/s)</b></td><td>Emits all K,V pairs in a tuple such that K,V is in the filter list
 * (or not in the list if inverse is set to true)</td><td>In-bound throughput and number of matching K,V pairs are the main determinant of performance.
 * Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); inverse=false; keyvals={a=2,d=5,e=2}</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for FilterKeyVals&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>filter</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td>{a=2}</td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5}</td><td>{a=2}</td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=1000,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,e=2}</td><td>{e=2}</td></tr>
 * <tr><td>Data (process())</td><td>{f=1,d=5,e=55,a=2}</td><td>{d=5}<br>{a=2}</td></tr>
 * <tr><td>Data (process())</td><td>{d=1,a=3,e=2}</td><td>{e=2}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 *
 */

public class FilterKeyVals<K,V> extends BaseKeyOperator<K>
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
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        entry.clear();
        entry.put(e.getKey(),e.getValue());
        boolean contains = keyvals.containsKey(entry);
        if ((contains && !inverse) || (!contains && inverse)) {
          HashMap<K, V> dtuple = new HashMap<K,V>(1);
          dtuple.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
          filter.emit(dtuple);
        }
      }
    }
  };

  @OutputPortFieldAnnotation(name="filter")
  public final transient DefaultOutputPort<HashMap<K, V>> filter = new DefaultOutputPort<HashMap<K, V>>(this);

  @NotNull()
  HashMap<HashMap<K,V>,Object> keyvals = new HashMap<HashMap<K,V>,Object>();
  boolean inverse = false;
  private transient HashMap<K,V> entry = new HashMap<K,V>(1);

  /**
   * getter function for parameter inverse
   * @return inverse
   */
  public boolean getInverse() {
    return inverse;
  }

  /**
   * True means match; False means unmatched
   * @param val
   */
  public void setInverse(boolean val) {
    inverse = val;
  }

  /**
   * True means match; False means unmatched
   * @return keyvals hash
   */
  @NotNull()
  public HashMap<HashMap<K,V>,Object> getKeyVals() {
    return keyvals;
  }

  /**
   * Adds a key to the filter list
   * @param map with key,val pairs to set as filters
   */
  public void setKeyVals(HashMap<K,V> map)
  {
    for (Map.Entry<K, V> e: map.entrySet()) {
      HashMap kvpair = new HashMap<K,V>(1);
      kvpair.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
      keyvals.put(kvpair, null);
    }
  }

  /*
   * Clears the filter list
   */
  public void clearKeys()
  {
    keyvals.clear();
  }

  /**
   * Clones V object. By default assumes immutable object (i.e. a copy is not made). If object is mutable, override this method
   * @param v value to be cloned
   * @return returns v as is (assumes immutable object)
   */
  public V cloneValue(V v)
  {
    return v;
  }
}
