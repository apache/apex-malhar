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

import org.apache.apex.malhar.lib.util.AbstractBaseFrequentKey;
import org.apache.apex.malhar.lib.util.UnifierArrayHashMapFrequent;
import org.apache.apex.malhar.lib.util.UnifierHashMapFrequent;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This operator filters the incoming stream of key value pairs by finding the key or keys (if there is a tie) that occur the fewest number of times within each window.&nbsp;
 * A list of the corresponding key value pairs are then output to the port named "list" and one of the corresponding key value pairs is output to the port "least", at the end of each window.
 * <p>
 * Occurrences of each key is counted and at the end of window any of the least frequent key is emitted on output port least and all least frequent
 * keys on output port list.
 * </p>
 * <p>
 * This module is an end of window module. In case of a tie any of the least key would be emitted. The list port would however have all the tied keys<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compared across application window(s). <br>
 * <b>Partitions : Yes, </b> least keys are unified on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V&gt;, V is ignored/not used<br>
 * <b>least</b>: emits HashMap&lt;K,Integer&gt;(1); where String is the least frequent key, and Integer is the number of its occurrences in the window<br>
 * <b>list</b>: emits ArrayList&lt;HashMap&lt;K,Integer&gt;(1)&gt;; Where the list includes all the keys are least frequent<br>
 * <br>
 * </p>
 *
 * @displayName Emit Least Frequent Tuple Key
 * @category Rules and Alerts
 * @tags filter, key value, count
 * @deprecated
 * @since 0.3.2
 */
@Deprecated
@OperatorAnnotation(partitionable = true)
public class LeastFrequentKeyMap<K, V> extends AbstractBaseFrequentKey<K>
{
  /**
   * The input port on which key value pairs are received.
   */
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Calls super.processTuple(tuple) for each key in the HashMap
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        processTuple(e.getKey());
      }
    }
  };

  /**
   * The output port on which one of the tuples,
   * which occurred the least number of times,
   * is emitted.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> least = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      Unifier<HashMap<K, Integer>> ret = new UnifierHashMapFrequent<K>();
      ((UnifierHashMapFrequent<K>)ret).setLeast(true);
      return ret;
    }
  };

  /**
   * The output port on which all the tuples,
   * which occurred the least number of times,
   * is emitted.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<ArrayList<HashMap<K, Integer>>> list = new DefaultOutputPort<ArrayList<HashMap<K, Integer>>>()
  {
    @Override
    public Unifier<ArrayList<HashMap<K, Integer>>> getUnifier()
    {
      Unifier<ArrayList<HashMap<K, Integer>>> ret = new UnifierArrayHashMapFrequent<K>();
      ((UnifierArrayHashMapFrequent<K>)ret).setLeast(true);
      return ret;
    }
  };

  /**
   * Emits tuple on port "least"
   *
   * @param tuple
   */
  @Override
  public void emitTuple(HashMap<K, Integer> tuple)
  {
    least.emit(tuple);
  }

  /**
   * Emits tuple on port "list"
   *
   * @param tlist
   */
  @Override
  public void emitList(ArrayList<HashMap<K, Integer>> tlist)
  {
    list.emit(tlist);
  }

  /**
   * returns val1 < val2
   *
   * @param val1
   * @param val2
   * @return val1 < val2
   */
  @Override
  public boolean compareCount(int val1, int val2)
  {
    return val1 < val2;
  }
}
