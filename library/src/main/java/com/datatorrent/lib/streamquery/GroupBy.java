/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.streamquery;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyValueOperator;
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
 * <br>
 *
 */
public class GroupBy<K,V> extends BaseKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data1")
  public final transient DefaultInputPort<HashMap<K,V>> data1 = new DefaultInputPort<HashMap<K,V>>()
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
  public final transient DefaultInputPort<HashMap<K,V>> data2 = new DefaultInputPort<HashMap<K,V>>()
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
  public final transient DefaultOutputPort<HashMap<K,V>> groupby = new DefaultOutputPort<HashMap<K,V>>();

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
