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
package org.apache.apex.malhar.lib.algo;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.apex.malhar.lib.util.BaseUniqueKeyCounter;
import org.apache.apex.malhar.lib.util.UnifierHashMapSumKeys;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator counts the number of times a tuple is received and emits
 * modified counts (if any) at the end of the streaming window.
 * <p>
 * Counts for modified keys are emitted at end of window in a single HashMap. If
 * no keys were received in a window, then nothing will be emitted. By default
 * the state is cleared at the end of the window. Cumulative counting can be
 * configured through the {@link UniqueCounter#setCumulative} property.
 * </p>
 * <p>
 * This is an end of window operator<br>
 * <br>
 * <b>StateFull : yes, </b> Tuples are aggregated over application window(s).
 * <br>
 * <b>Partitions : Yes, </b> Unique count is unified at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>count</b>: emits HashMap&lt;K,Integer&gt;<br>
 * <b>Properties</b>: None<br>
 * <br>
 * </p>
 *
 * @displayName Count Unique Tuples
 * @category Stats and Aggregations
 * @tags count
 *
 * @since 0.3.2
 */

@OperatorAnnotation(partitionable = true)
public class UniqueCounter<K> extends BaseUniqueKeyCounter<K>
{
  private boolean cumulative;
  HashSet<K> inputSet = new HashSet<>();

  /**
   * The input port which receives incoming tuples.
   */
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(K tuple)
    {
      inputSet.add(tuple);
      processTuple(tuple);
    }
  };

  /**
   * The output port which emits a map from keys to the number of times they occurred within an application window.
   */
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      UnifierHashMapSumKeys<K, Integer> unifierHashMapSumKeys =  new UnifierHashMapSumKeys<>();
      unifierHashMapSumKeys.setType(Integer.class);
      return unifierHashMapSumKeys;
    }
  };

  /**
   * Emits only the keys and values changed or added in a given window.
   */
  @Override
  public void endWindow()
  {
    HashMap<K, Integer> tuple = new HashMap<>();
    for (K key: inputSet) {
      tuple.put(key, map.get(key).toInteger());
    }
    if (!tuple.isEmpty()) {
      count.emit(tuple);
    }
    if (!cumulative) {
      map.clear();
    }
    inputSet.clear();
  }

  /**
   * Gets the cumulative mode.
   * @return The cumulative mode.
   */
  public boolean isCumulative()
  {
    return cumulative;
  }

  /**
   * If enabled then the unique keys is counted and maintained in memory for the life of the operator. If not enabled
   * keys are counted a per window bases.<br/>
   * <b>Note:</b> If cumulative mode is enabled and the operator receives many unique keys, then this operator
   * could eventually run out of memory.
   * @param cumulative
   */
  public void setCumulative(boolean cumulative)
  {
    this.cumulative = cumulative;
  }
}
