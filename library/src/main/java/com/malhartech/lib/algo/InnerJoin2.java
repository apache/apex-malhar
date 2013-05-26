/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
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
  public final transient DefaultOutputPort<MultiValueMap> result = new DefaultOutputPort<MultiValueMap>(this);

  @InputPortFieldAnnotation(name = "data1")
  public final transient DefaultInputPort<HashMap<K,V>> data1 = new DefaultInputPort<HashMap<K,V>>(this)
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
  public final transient DefaultInputPort<HashMap<K,V>> data2 = new DefaultInputPort<HashMap<K,V>>(this)
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
