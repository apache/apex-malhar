/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.misc.math;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.util.BaseKeyValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.UnifierCountOccurKey;
import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This Operator aggregates occurrence of keys in &lt;key,value&gt; pair at input port.&lt;Key,Occurrence count&gt; pair is emitted for each input on output port.
 * <p>
 * <br>
 * StateFull : Yes, key occurrence is aggregated over windows. <br>
 * Partitions : Yes, count occurrence unifier at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V&gt;<br>
 * <b>count</b>: emits KeyValPair&lt;K,Integer&gt;</b><br>
 * <br>
 * @displayName Count Key Value
 * @category Math
 * @tags count, key value, aggregate
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
public class CountKeyVal<K, V> extends BaseKeyValueOperator<K, V>
{

  /**
   * Key occurrence count map.
   */
  protected HashMap<K, MutableInt> counts = new HashMap<K, MutableInt>();

  /**
   * Input data port that takes key value pair.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * For each tuple (a key value pair): Adds the values for each key, Counts
     * the number of occurrence of each key
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      MutableInt count = counts.get(key);
      if (count == null) {
        count = new MutableInt(0);
        counts.put(cloneKey(key), count);
      }
      count.increment();
    }

    @Override
    public StreamCodec<KeyValPair<K, V>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }
  };

  /**
   * Key, occurrence value pair output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Integer>> count = new DefaultOutputPort<KeyValPair<K, Integer>>()
  {
    @Override
    public UnifierCountOccurKey<K> getUnifier()
    {
      return new UnifierCountOccurKey<K>();
    }
  };

  /**
   * Emits on all ports that are connected. Data is computed during process on
   * input port and endWindow just emits it for each key. Clears the internal
   * data if resetAtEndWindow is true.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, MutableInt> e : counts.entrySet()) {
      count.emit(new KeyValPair(e.getKey(),
          new Integer(e.getValue().intValue())));
    }
    counts.clear();
  }
}
