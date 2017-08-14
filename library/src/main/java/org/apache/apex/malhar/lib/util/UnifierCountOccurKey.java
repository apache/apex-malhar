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
 * This unifier counts the number of times it consumes an input tuple within each application window.&nbsp;
 * At the end of each window the tuples and their counts are emitted as a map.
 * <p>
 * The processing is done with sticky key partitioning, i.e. each one key belongs only to one partition.
 * </p>
 * @displayName Unifier Count Occur Key
 * @category Algorithmic
 * @tags numeric
 * @since 0.3.3
 */
public class UnifierCountOccurKey<K> implements Unifier<KeyValPair<K, Integer>>
{
  /**
   * Key/Occurrence  map used for unifying key/occurrence values.
   */
  private HashMap<K, Integer> counts = new HashMap<K, Integer>();

  /**
   * Key/occurrence value pair output port.
   */
  public final transient DefaultOutputPort<KeyValPair<K, Integer>> outport = new DefaultOutputPort<KeyValPair<K, Integer>>();

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * This is a merge metric for operators that use sticky key partition
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(KeyValPair<K, Integer> tuple)
  {
    if (counts.containsKey(tuple.getKey())) {
      Integer val = (Integer)counts.remove(tuple.getKey());
      counts.put(tuple.getKey(), val + tuple.getValue());
    } else {
      counts.put(tuple.getKey(), tuple.getValue());
    }
  }

  /**
   * empty
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
  }


  /**
   * emits count sum if it is not empty
   */
  @Override
  public void endWindow()
  {
    if (!counts.isEmpty())  {
      for (Map.Entry<K, Integer> entry : counts.entrySet()) {
        outport.emit(new KeyValPair<K, Integer>(entry.getKey(), entry.getValue()));
      }
    }
    counts = new HashMap<K, Integer>();
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
