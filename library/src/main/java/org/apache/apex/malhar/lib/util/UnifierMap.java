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
 * This unifier combines all the maps it receives within an application window,
 * and emits the combined map at the end of the application window.
 * <p>
 * This unifier uses round robin partitioning.
 * </p>
 * @displayName Unifier Map
 * @category Algorithmic
 * @tags key value
 * @since 0.3.4
 */
public class UnifierMap<K, V> implements Unifier<Map<K, V>>
{
  public Map<K, V> mergedTuple = new HashMap<K, V>();
  /**
   * This is the output port which emits a merged map.
   */
  public final transient DefaultOutputPort<Map<K, V>> mergedport = new DefaultOutputPort<Map<K, V>>();

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * This is a merge metric for operators that use sticky key partition
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(Map<K, V> tuple)
  {
    mergedTuple.putAll(tuple);
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
      mergedTuple = new HashMap<K, V>();
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
