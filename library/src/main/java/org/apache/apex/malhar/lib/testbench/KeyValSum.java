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
package org.apache.apex.malhar.lib.testbench;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator consumes maps whose keys are strings and values are integers.&nbsp;
 * In each application window the values for each key are summed and each string/sum pair is emitted at the end of each window.
 * <p></p>
 * @displayName Key Val Sum
 * @category Test Bench
 * @tags count
 * @since 0.3.2
 */
public class KeyValSum extends BaseOperator
{
  private Map<String, Integer> collect;

  /**
   * This input port on which tuples are received.
   */
  public final transient DefaultInputPort<Map<String, Integer>> inport = new DefaultInputPort<Map<String, Integer>>()
  {
    @Override
    public void process(Map<String, Integer> s)
    {
      for (Map.Entry<String, Integer> entry : s.entrySet()) {
        if (collect.containsKey(entry.getKey())) {
          Integer value = (Integer)collect.remove(entry.getKey());
          collect.put(entry.getKey(), value + entry.getValue());
        } else {
          collect.put(entry.getKey(), entry.getValue());
        }
      }
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    collect  = new HashMap<String, Integer>();
  }

  /**
   * The output port on which sums are emitted.
   */
  public final transient DefaultOutputPort<Map<String, Integer>> outport = new DefaultOutputPort<Map<String, Integer>>();

  @Override
  public void endWindow()
  {
    outport.emit(collect);
  }
}
