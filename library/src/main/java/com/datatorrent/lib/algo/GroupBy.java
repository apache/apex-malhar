/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.util.BaseKeyValueOperator;
import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

/**
 *
 * Takes two streams, and emits groupby result on port groupby<p>
 * <br>
 * This module produces continuous tuples. At end of window all data is flushed<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data1</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>data2</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>groupby</b>: emits HashMap&lt;K,V&gt;(1)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key to "groupby"<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * <b>key</b> cannot be empty<br>
 * <br>
 * <b>Specific run time checks are</b>: None<br>
 * All incoming tuples must include the key to groupby, else the tuple is dropped<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode; Number will vary based on data set<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for GroupBy&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 20K in-bound tuples/s (each with two k,v pairs) and 6 million out-bound tuples/s</b></td>
 * <td>Emits one tuple per match in both the sets per window</td>
 * <td>In-bound throughput on both ports and out-bound throughput (number of matches) are the main determinant of performance.</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); key=a</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for GroupBy&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th colspan=2>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data1</i>(HashMap&lt;K,V&gt;)</th><th><i>data2</i>(HashMap&lt;K,V&gt;)</th><th><i>groupby</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=4}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1000,b=1}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>{a=2,c=3}</td><td>{a=2,b=5,c=3}</td></tr>
 * <tr><td>Data (process())</td><td></td><td>{a=2,c=33}</td><td>{a=2,b=5,c=33}</td></tr>
 * <tr><td>Data (process())</td><td></td><td>{a=1000,c=34}</td><td>{a=1000,b=1,c=34}</td></tr>
 * <tr><td>Data (process())</td><td></td><td>{a=1000,c=4}</td><td>{a=1000,b=1,c=4}</td></tr>
 * <tr><td>Data (process())</td><td>{a=1000,b=2}</td><td></td><td>{a=1000,b=2,c=34}<br>{a=1000,b=2,c=4}</td></tr>
 * <tr><td>Data (process())</td><td>{b=2}</td><td></td><td><br></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 * @author amol<br>
 *
 */
public class GroupBy<K,V> extends BaseKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data1")
  public final transient DefaultInputPort<HashMap<K,V>> data1 = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * Checks if key exists. If so emits all current combinations with matching tuples received on port "data2"
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      V val = tuple.get(key);
      if (val == null) { // emit error tuple
        return;
      }
      emitTuples(tuple, map2.get(val), val);
      registerTuple(tuple, map1, val);
    }
  };
  @InputPortFieldAnnotation(name = "data2")
  public final transient DefaultInputPort<HashMap<K,V>> data2 = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * Checks if key exists. If so emits all current combinations with matching tuples received on port "data1"
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      V val = tuple.get(key);
      if (val == null) { // emit error tuple
        return;
      }
      emitTuples(tuple, map1.get(val), val);
      registerTuple(tuple, map2, val);
    }
  };
  @OutputPortFieldAnnotation(name = "groupby")
  public final transient DefaultOutputPort<HashMap<K,V>> groupby = new DefaultOutputPort<HashMap<K,V>>(this);

  /**
   * Adds tuples to the list associated with its port
   * @param tuple
   * @param map
   * @param val
   */
  protected void registerTuple(HashMap<K,V> tuple, HashMap<V,ArrayList<HashMap<K,V>>> map, V val)
  {
    // Construct the data (HashMap) to be inserted into sourcemap
    HashMap<K,V> data = new HashMap<K,V>();
    for (Map.Entry<K,V> e: tuple.entrySet()) {
      if (!e.getKey().equals(key)) {
        data.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
      }
    }
    ArrayList<HashMap<K,V>> list = map.get(val);
    if (list == null) {
      list = new ArrayList<HashMap<K,V>>();
      map.put(val, list);
    }
    list.add(data);
  }

  @NotNull
  K key;
  protected HashMap<V,ArrayList<HashMap<K,V>>> map1 = new HashMap<V,ArrayList<HashMap<K,V>>>();
  protected HashMap<V,ArrayList<HashMap<K,V>>> map2 = new HashMap<V,ArrayList<HashMap<K,V>>>();

  /**
   * Sets key to groupby
   * @param str
   */
  public void setKey(K str)
  {
    key = str;
  }

  @NotNull
  public K getKey()
  {
    return key;
  }

  /**
   * Emits all combinations of source and matching other list
   * @param source
   * @param list
   * @param val
   */
  public void emitTuples(HashMap<K,V> source, ArrayList<HashMap<K,V>> list, V val)
  {
    if (list == null) { // The currentList does not have the value yet
      return;
    }

    HashMap<K,V> tuple;
    for (HashMap<K,V> e: list) {
      tuple = new HashMap<K, V>();
      tuple.put(key, val);
      for (Map.Entry<K,V> o: e.entrySet()) {
        tuple.put(cloneKey(o.getKey()), cloneValue(o.getValue()));
      }
      for (Map.Entry<K,V> o: source.entrySet()) {
        if (!o.getKey().equals(key)) {
          tuple.put(cloneKey(o.getKey()), cloneValue(o.getValue()));
        }
      }
      groupby.emit(tuple);
    }
  }

  /**
   * Clears internal data
   */
  @Override
  public void endWindow()
  {
    map1.clear();
    map2.clear();
  }
}
