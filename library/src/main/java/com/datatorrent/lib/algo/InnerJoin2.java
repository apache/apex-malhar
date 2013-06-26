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
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyValueOperator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.commons.collections.map.MultiValueMap;

/**
 * <b>Not certified</b>
 * <b>Do Not Use</b>
 */
public class InnerJoin2 <K,V> extends BaseKeyValueOperator<K,V>
{
  public HashMap<K, ArrayList<HashMap<K,V>>> map1 = new HashMap<K, ArrayList<HashMap<K,V>>>();
  public HashMap<K, ArrayList<HashMap<K,V>>> map2 = new HashMap<K, ArrayList<HashMap<K,V>>>();

  @OutputPortFieldAnnotation(name = "result")
  public final transient DefaultOutputPort<MultiValueMap> result = new DefaultOutputPort<MultiValueMap>();

  @InputPortFieldAnnotation(name = "data1")
  public final transient DefaultInputPort<HashMap<K,V>> data1 = new DefaultInputPort<HashMap<K,V>>()
  {
    /**
     * Checks if key exists. If so emits all current combinations with matching tuples received on port "data2"
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      Iterator it = tuple.entrySet().iterator();
      if( it.hasNext() ) {
        Entry<K,V> e = (Entry)it.next();
        emitTuples(tuple, map2.get(e.getKey()));
        registerTuple(tuple, map1, e.getKey());
      }
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
      Iterator it = tuple.entrySet().iterator();
      if( it.hasNext() ) {
        Entry<K,V> e = (Entry)it.next();
        ArrayList<HashMap<K,V>> list = map1.get(e.getKey());

        emitTuples(tuple, list);
        registerTuple(tuple, map2, e.getKey());
      }
    }
  };

  public void emitTuples(HashMap<K,V> row, ArrayList<HashMap<K,V>> list) {
    if( list == null )
      return;
    Iterator it = row.entrySet().iterator();
    Entry<K,V> e = (Entry)it.next();
    for( HashMap<K,V> map : list ) {
      MultiValueMap multimap = new MultiValueMap(); // original K,V pair
      multimap.put(e.getKey(), e.getValue());

      Iterator it1 = map.entrySet().iterator(); // the other side K,V pair
      Entry<K,V> e1 = (Entry)it1.next();
      multimap.put(e1.getKey(), e1.getValue());
      result.emit(multimap);
    }
  }

  public void registerTuple(HashMap<K,V> tuple, HashMap<K, ArrayList<HashMap<K,V>>> map, K key) {
    ArrayList<HashMap<K,V>> list = map.get(key);
    if( list == null ) {
      list = new ArrayList<HashMap<K,V>>();
      map.put(key, list);
    }
    if( !list.contains(tuple) ) {
      list.add(tuple);
    }
  }

  /**
   * Clears cache/hash for both ports
   */
  @Override
  public void endWindow()
  {
    map1.clear();
    map2.clear();
  }
}
