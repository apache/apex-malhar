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
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;

/**
 * This unifier is used by the LeastFrequentKey operator in order to merge the output of the "least" output port.
 * <p></p>
 * @displayName Emit Least Frequent Key Unifier
 * @category Algorithmic
 * @tags filter, key value, count
 *
 * @since 0.3.3
 */
public class LeastFrequentKeyUnifier<K> implements Unifier<HashMap<K, Integer>>
{
  /**
   * Key/Least value map.
   */
  private HashMap<K, Integer> leastMap  = new HashMap<K, Integer>();

  /**
   * The output port on which the least frequent tuple is emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, Integer>> mergedport = new DefaultOutputPort<HashMap<K, Integer>>();

  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    mergedport.emit(leastMap);
    leastMap.clear();
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void process(HashMap<K, Integer> tuple)
  {
    for (Map.Entry<K, Integer> entry : tuple.entrySet()) {
      Integer value = entry.getValue();
      if (leastMap.containsKey(entry.getKey())) {
        if (leastMap.get(entry.getKey()) < value) {
          value = leastMap.remove(entry.getKey());
        } else {
          leastMap.remove(entry.getKey());
        }
      }
      leastMap.put(entry.getKey(), value);
    }
  }

}
