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
package org.apache.apex.malhar.contrib.misc.algo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.util.BaseKeyValueOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator takes a stream of key value pairs each window,
 * and outputs a set of inverted key value pairs at the end of each window.&nbsp;
 * The values in the key value pairs received by this operator are an array lists, which may multiple values.
 * <p>
 * Inverts the index and sends out the tuple on output port "index" at the end of the window.
 * </p>
 * <p>
 * This is an end of window operator<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compare across application window(s). <br>
 * <b>Partitions : Yes, </b> inverted indexes are unified by instance of same operator. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,ArrayList&lt;V&gt;&gt;<br>
 * <b>index</b>: emits HashMap&lt;V,ArrayList&lt;K&gt;&gt;(1), one HashMap per V<br>
 * <br>
 * </p>
 *
 * @displayName Invert Key Value Pairs (Array)
 * @category Stream Manipulators
 * @tags key value
 *
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
@OperatorAnnotation(partitionable = true)
public class InvertIndexArray<K, V> extends BaseKeyValueOperator<K,V>
{
  /**
   * Inverted key/value map.
   */
  protected HashMap<V, ArrayList<K>> map = new HashMap<V, ArrayList<K>>();

  /**
   * The input port on which key value pairs are received.
   */
  public final transient DefaultInputPort<HashMap<K, ArrayList<V>>> data = new DefaultInputPort<HashMap<K, ArrayList<V>>>()
  {
    /**
     * Reverse indexes a HashMap<K, ArrayList<V>> tuple
     */
    @Override
    public void process(HashMap<K, ArrayList<V>> tuple)
    {
      for (Map.Entry<K, ArrayList<V>> e: tuple.entrySet()) {
        ArrayList<V> alist = e.getValue();
        if (alist == null) { // error tuple?
          continue;
        }
        for (V val : alist) {
          insert(val, cloneKey(e.getKey()));
        }
      }
    }
  };

  /**
   * The output port or which inverted key value pairs are emitted.
   */
  public final transient DefaultOutputPort<HashMap<V, ArrayList<K>>> index = new DefaultOutputPort<HashMap<V, ArrayList<K>>>()
  {
    @Override
    public Unifier<HashMap<V, ArrayList<K>>> getUnifier()
    {
      return new InvertIndex<K, V>();
    }
  };

  /**
   *
   * Returns the ArrayList stored for a key
   *
   * @param key
   */
  void insert(V val, K key)
  {
    ArrayList<K> list = map.get(val);
    if (list == null) {
      list = new ArrayList<K>(4);
      map.put(cloneValue(val), list);
    }
    list.add(key);
  }

  /**
   * Emit all the data and clear the hash
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<V, ArrayList<K>> e: map.entrySet()) {
      HashMap<V, ArrayList<K>> tuple = new HashMap<V, ArrayList<K>>(1);
      tuple.put(e.getKey(), e.getValue());
      index.emit(tuple);
    }
    map.clear();
  }
}
