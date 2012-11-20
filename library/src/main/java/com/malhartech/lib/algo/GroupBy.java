/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyValueOperator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

/**
 *
 * Takes two streams, and emits groupby result as per property Key<p>
 * <br>
 * Even though this module produces continuous tuples, at end of window all data is flushed. Thus the data set is windowed
 * and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data1</b>: expects HashMap<K,V><br>
 * <b>data2</b>: expects HashMap<K,V><br>
 * <b>groupby</b>: emits HashMap<K,V><br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key to "groupby"<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <b>key</b> cannot be empty<br>
 * <br>
 * Run time checks are:<br>
 * All incoming tuples must include the key to groupby
 * <br>
 *
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
  HashMap<V,ArrayList<HashMap<K,V>>> map1 = new HashMap<V,ArrayList<HashMap<K,V>>>();
  HashMap<V,ArrayList<HashMap<K,V>>> map2 = new HashMap<V,ArrayList<HashMap<K,V>>>();

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
   * Clears cache/hash for both ports
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    map1.clear();
    map2.clear();
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
}
