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
package org.apache.apex.examples.distributeddistinct;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * An operator that takes two streams of Integer to Integer KeyValPairs and verifies that the two streams output the
 * same KeyValPairs within a given window.
 *
 * @since 1.0.4
 */
public class CountVerifier implements Operator
{
  Map<Integer, Integer> trueCount = new HashMap<Integer, Integer>();
  Map<Integer, Integer> receivedCount = new HashMap<Integer, Integer>();

  public final transient DefaultInputPort<KeyValPair<Integer, Integer>> trueIn = new DefaultInputPort<KeyValPair<Integer, Integer>>()
  {
    @Override
    public void process(KeyValPair<Integer, Integer> tuple)
    {
      trueCount.put(tuple.getKey(), tuple.getValue());
    }
  };

  public final transient DefaultInputPort<KeyValPair<Integer, Integer>> recIn = new DefaultInputPort<KeyValPair<Integer, Integer>>()
  {
    @Override
    public void process(KeyValPair<Integer, Integer> tuple)
    {
      receivedCount.put(tuple.getKey(), tuple.getValue());
    }
  };

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Integer> successPort = new DefaultOutputPort<Integer>();
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Integer> failurePort = new DefaultOutputPort<Integer>();

  @Override
  public void setup(OperatorContext arg0)
  {

  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void beginWindow(long windowID)
  {
  }

  /**
   * Checks that the key to value pairs are the same and counts the number of pairs that are different. If there are
   * failures, it will emit the number of failures to the failure port. Otherwise, it will emit the number of keys to
   * the success port.
   */
  @Override
  public void endWindow()
  {
    int failureCount = 0;
    for (Map.Entry<Integer, Integer> e : receivedCount.entrySet()) {
      Integer key = e.getKey();
      if (!trueCount.get(key).equals(e.getValue())) {
        failureCount++;
      }
    }
    if (failureCount != 0) {
      failurePort.emit(failureCount);
    } else {
      successPort.emit(trueCount.size());
    }
  }
}
