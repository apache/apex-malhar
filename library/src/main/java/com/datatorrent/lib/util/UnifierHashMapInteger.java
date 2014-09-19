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
package com.datatorrent.lib.util;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Combiner for an output port that emits object with Map<K,Integer> interface and has the processing done
 * with round robin partitions. The combiner needs to add values of a key from every partition
 *
 * @since 0.3.2
 */
public class UnifierHashMapInteger<K> implements Unifier<HashMap<K, Integer>>
{
  public HashMap<K, Integer> mergedTuple = new HashMap<K, Integer>();
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
      }
      else {
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
