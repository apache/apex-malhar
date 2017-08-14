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
package org.apache.apex.malhar.lib.util;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;

/**
 * This unifier consumes tuples which are maps from objects to integers.&nbsp;
 * The integers for each key are aggregated and a map from keys to sums is emitted at the end of each application window.
 * <p>
 * The processing is done with sticky key partitioning, i.e. each one key belongs only to one partition.
 * </p>
 * @displayName Unifier Hash Map Integer
 * @category Algorithmic
 * @tags numeric
 * @since 0.3.2
 */
public class UnifierHashMapInteger<K> implements Unifier<HashMap<K, Integer>>
{
  public HashMap<K, Integer> mergedTuple = new HashMap<K, Integer>();
  /**
   * This is the output port which emits key value pairs which map keys to sums.
   */
  public final transient DefaultOutputPort<HashMap<K, Integer>> mergedport = new DefaultOutputPort<HashMap<K, Integer>>();

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(HashMap<K, Integer> tuple)
  {
    for (Map.Entry<K, Integer> e: tuple.entrySet()) {
      Integer val = mergedTuple.get(e.getKey());
      if (val == null) {
        val = e.getValue();
      } else {
        val += e.getValue();
      }
      mergedTuple.put(e.getKey(), val);
    }
  }

  /**
   * a no op
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
  }


  /**
   * emits mergedTuple on mergedport if it is not empty
   */
  @Override
  public void endWindow()
  {
    if (!mergedTuple.isEmpty())  {
      mergedport.emit(mergedTuple);
      mergedTuple = new HashMap<K, Integer>();
    }
  }

  /**
   * a no-op
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * a noop
   */
  @Override
  public void teardown()
  {
  }
}
