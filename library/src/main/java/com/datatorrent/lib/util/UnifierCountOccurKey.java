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
 * This unifier aggregates occurrence of key on output port and emits key/total occurrence value pair.
 *
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
