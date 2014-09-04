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
package com.datatorrent.demos.uniquecount;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.KeyHashValPair;

import java.util.HashMap;
import java.util.Map;

/*
Compare results and print non-matching values to console.
 */
/**
 * <p>CountVerifier class.</p>
 *
 * @since 1.0.2
 */
public class CountVerifier<K> implements Operator
{
  HashMap<K, Integer> map1 = new HashMap<K, Integer>();
  HashMap<K, Integer> map2 = new HashMap<K, Integer>();

  public transient final DefaultInputPort<KeyHashValPair<K, Integer>> in1 =
      new DefaultInputPort<KeyHashValPair<K, Integer>>()
      {
        @Override
        public void process(KeyHashValPair<K, Integer> tuple)
        {
          processTuple(tuple, map1);
        }
      };

  public transient final DefaultInputPort<KeyHashValPair<K, Integer>> in2 =
      new DefaultInputPort<KeyHashValPair<K, Integer>>()
      {
        @Override
        public void process(KeyHashValPair<K, Integer> tuple)
        {
          processTuple(tuple, map2);
        }
      };

  void processTuple(KeyHashValPair<K, Integer> tuple, HashMap<K, Integer> map)
  {
    map.put(tuple.getKey(), tuple.getValue());
  }

  @OutputPortFieldAnnotation(name = "success", optional=true)
  public transient final DefaultOutputPort<Integer> successPort = new DefaultOutputPort<Integer>();
  @OutputPortFieldAnnotation(name = "failure", optional=true)
  public transient final DefaultOutputPort<Integer> failurePort = new DefaultOutputPort<Integer>();

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {
    int failureCount = 0;
    for (Map.Entry<K, Integer> e : map1.entrySet()) {
      K key = e.getKey();
      int val = map2.get(key);
      if (val != e.getValue()) {
        failureCount++;
      }
    }
    if (failureCount != 0) {
      failurePort.emit(failureCount);
    } else {
      successPort.emit(map1.size());
    }
  }

  @Override
  public void setup(Context.OperatorContext operatorContext)
  {

  }

  @Override
  public void teardown()
  {

  }
}
