/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.AbstractBaseFrequentKey;
import com.datatorrent.lib.util.UnifierArrayHashMapFrequent;
import com.datatorrent.lib.util.UnifierHashMapFrequent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This operator filters the incoming stream of key value pairs by finding the key or keys (if there is a tie)
 * that occur the largest number of times within each window.&nbsp;
 * A list of the corresponding key value pairs are then output to the port named "list" and one of the corresponding key value pairs is output to the port "most", at the end of each window.
 * <p>
 * Occurrences of each key is counted and at the end of window any of the most frequent key is emitted on output port least and all least frequent
 * keys on output port list.
 * </p>
 * <p>
 * This module is an end of window module. In case of a tie any of the least key would be emitted. The list port would however have all the tied keys<br>
 * <br>
 *  <b>StateFull : Yes</b>, Values are compared all over  application window can be > 1. <br>
 *  <b>Partitions : Yes</b>, Result is unified on output port. <br>
 *  <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V&gt;, V is ignored/not used<br>
 * <b>most</b>: emits HashMap&lt;K,Integer&gt;(1); where String is the least frequent key, and Integer is the number of its occurrences in the window<br>
 * <b>list</b>: emits ArrayList&lt;HashMap&lt;K,Integer&gt;(1)&gt;; Where the list includes all the keys are least frequent<br>
 * <br>
 * </p>
 *
 * @displayName Emit Most Frequent Key
 * @category algorithm
 * @tags filter, key value, count
 *
 * @since 0.3.2
 */

@OperatorAnnotation(partitionable = true)
public class MostFrequentKeyMap<K,V> extends AbstractBaseFrequentKey<K>
{
  /**
   * The input port which receives incoming key value pairs.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K,V>> data = new DefaultInputPort<Map<K,V>>()
  {
    /**
     * Calls super.processTuple(tuple) for each key in the HashMap
     */
    @Override
    public void process(Map<K,V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        processTuple(e.getKey());
      }
    }
  };
  /**
   * The output port on which all the tuples,
   * which occurred the most number of times,
   * is emitted.
   */
  @OutputPortFieldAnnotation(name = "most")
  public final transient DefaultOutputPort<HashMap<K, Integer>> most = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      Unifier<HashMap<K, Integer>> ret = new UnifierHashMapFrequent<K>();
      ((UnifierHashMapFrequent<K>) ret).setLeast(false);
      return ret;
    }
  };


  @OutputPortFieldAnnotation(name = "list")
  public final transient DefaultOutputPort<ArrayList<HashMap<K, Integer>>> list = new DefaultOutputPort<ArrayList<HashMap<K, Integer>>>()
  {
    @SuppressWarnings("rawtypes")
    @Override
    public Unifier<ArrayList<HashMap<K, Integer>>> getUnifier()
    {
      Unifier<ArrayList<HashMap<K, Integer>>> ret = new UnifierArrayHashMapFrequent<K>();
      ((UnifierHashMapFrequent) ret).setLeast(false);
      return ret;
    }
  };


  /**
   * Emits tuple on port "most"
   * @param tuple
   */
  @Override
  public void emitTuple(HashMap<K, Integer> tuple)
  {
    most.emit(tuple);
  }

  /**
   * Emits tuple on port "list"
   * @param tlist
   */
  @Override
  public void emitList(ArrayList<HashMap<K, Integer>> tlist)
  {
    list.emit(tlist);
  }

  /**
   * returns val1 < val2
   * @param val1
   * @param val2
   * @return val1 > val2
   */
  @Override
  public boolean compareCount(int val1, int val2)
  {
    return val1 > val2;
  }
}
