/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyValueOperator;
import com.malhartech.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class RoundRobinHashMap<K, V> extends BaseKeyValueOperator<K, V>
{
  protected K[] keys;
  protected int cursor = 0;
  private HashMap<K,V> otuple;

  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    /**
     * Emits key, key/val pair, and val based on port connections
     */
    @Override
    public void process(V tuple)
    {
      if (keys.length == 0) {
        return;
      }
      if (cursor == 0) {
        otuple = new HashMap<K,V>();
      }
      otuple.put(keys[cursor], tuple);
      if (++cursor >= keys.length) {
        map.emit(otuple);
        cursor = 0;
      }
    }
  };
  @OutputPortFieldAnnotation(name = "map")
  public final transient DefaultOutputPort<HashMap<K, V>> map = new DefaultOutputPort<HashMap<K, V>>(this);

  public void setKeys(K[] keys) {
    this.keys = keys;
  }
}
