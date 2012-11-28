/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Inverts the map and sends out the tuple on output port index on end of window. This operator is demo specific and should be moved<p>
 * Has been templatized, but not tested<br>
 *
 *
 *
 * @author amol<br>
 *
 */
public class InvertIndexUniqueMap<K,V> extends BaseOperator
{
  private static Logger log = LoggerFactory.getLogger(InvertIndexUniqueMap.class);
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    @Override
    public void process(HashMap<K,V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        HashMap<K, Object> values = map.get(e.getValue());
        if (values == null) {
          values = new HashMap<K, Object>(4); // start with 4 slots, keep it low
          map.put(e.getValue(), values);
        }
        values.put(e.getKey(), null);

        // Now remove the key from old map value
        V cur_key = secondary_index.get(e.getKey());
        if ((cur_key != null) && !cur_key.equals(e.getValue())) { // remove from old map
          values = map.get(cur_key);
          if (values != null) { // must be true
            values.remove(e.getKey());
          }
          if (values.isEmpty()) { // clean up memory if need be
            map.remove(cur_key);
          }
        }
        secondary_index.put(e.getKey(), e.getValue());
      }
    }
  };
  @OutputPortFieldAnnotation(name = "index")
  public final transient DefaultOutputPort<HashMap<V, ArrayList>> index = new DefaultOutputPort<HashMap<V, ArrayList>>(this);

  HashMap<V, HashMap<K, Object>> map = new HashMap<V, HashMap<K, Object>>();
  HashMap<K, V> secondary_index = new HashMap<K, V>(5);

  protected boolean hasIndex(String key)
  {
    HashMap<K, Object> val = map.get(key);
    return (val != null) && !val.isEmpty();
  }

  protected boolean hasSecondaryIndex(String key)
  {
    return (secondary_index.get(key) != null);
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<V, HashMap<K, Object>> e: map.entrySet()) {
      ArrayList keys = new ArrayList();
      for (Map.Entry<K, Object> o: e.getValue().entrySet()) {
        keys.add(o.getKey());
      }
      HashMap<V, ArrayList> tuple = new HashMap<V, ArrayList>(1);
      tuple.put(e.getKey(), keys);
      index.emit(tuple);
    }
  }
}
