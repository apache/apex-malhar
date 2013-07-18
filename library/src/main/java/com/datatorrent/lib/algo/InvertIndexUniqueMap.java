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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * <b>Demo only module. do not use</b>
 * Inverts the map and sends out the tuple on output port index on end of window. This operator is demo specific and should be moved<p>
 * Has been templatized, but not tested<br>
 * TBD: Move it to demo library
 *
 */
public class InvertIndexUniqueMap<K,V> extends BaseOperator
{
  /**
   * Input port.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>()
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
  @SuppressWarnings("rawtypes")
  @OutputPortFieldAnnotation(name = "index")
  public final transient DefaultOutputPort<HashMap<V, ArrayList>> index = new DefaultOutputPort<HashMap<V, ArrayList>>();

  HashMap<V, HashMap<K, Object>> map = new HashMap<V, HashMap<K, Object>>();
  HashMap<K, V> secondary_index = new HashMap<K, V>(5);

  protected boolean hasIndex(V key)
  {
    HashMap<K, Object> val = map.get(key);
    return (val != null) && !val.isEmpty();
  }

  protected boolean hasSecondaryIndex(K key)
  {
    return (secondary_index.get(key) != null);
  }

  /**
   * Emit all the data and clear the hash
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
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
