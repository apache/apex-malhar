/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

import com.datatorrent.lib.util.AbstractBaseNOperatorMap;

/**
 * This operator filters the incoming stream of key value pairs by emitting the first N key value pairs with a specified key in each window.
 * <p>
 * Emits first N tuples of a particular key.
 * </p>
 * <p>
 * This module is a pass through module<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compare across application window(s). <br>
 * <b>Partitions : No, </b> will yield wrong results. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input data port expects HashMap&lt;K,V&gt;<br>
 * <b>bottom</b>: Output data port, emits HashMap&lt;K,V&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * N: Has to be >= 1<br>
 * <br>
 * <br>
 * </p>
 *
 * @displayName First N Keyval Pairs Matching Key
 * @category Algorithmic
 * @tags filter, key value
 *
 * @since 0.3.2
 */
@OperatorAnnotation(partitionable = false)
public class FirstN<K,V> extends AbstractBaseNOperatorMap<K, V>
{
  /**
   * key count map.
   */
  HashMap<K, MutableInt> keycount = new HashMap<K, MutableInt>();

  /**
   * Inserts tuples into the queue
   * @param tuple to insert in the queue
   */
  @Override
  public void processTuple(Map<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      MutableInt count = keycount.get(e.getKey());
      if (count == null) {
        count = new MutableInt(0);
        keycount.put(e.getKey(), count);
      }
      count.increment();
      if (count.intValue() <= getN()) {
        first.emit(cloneTuple(e.getKey(), e.getValue()));
      }
    }
  }

  /**
   * The output port on which the first N key value pairs are emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> first = new DefaultOutputPort<HashMap<K, V>>();

  /**
   * Clears the cache to start anew in a new window
   */
  @Override
  public void endWindow()
  {
    keycount.clear();
  }
  
  /**
   * First N number of KeyValue pairs for each Key.
   * @param val
   */
  public void setN(int val)
  {
   super.setN(val);
  }
}
