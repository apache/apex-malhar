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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>A base implementation of an operator which does count occurrence.</p>
 * <p>
 * @displayName Count Occurrence
 * @category Test Bench
 * @tags count
 * @since 0.3.2
 */
public class CountOccurance<k> extends BaseOperator
{
  private Map<k, Integer> collect;
  public final transient DefaultInputPort<k> inport = new DefaultInputPort<k>()
  {
    @Override
    public void process(k s)
    {
      if (collect.containsKey(s)) {
        Integer value = (Integer)collect.remove(s);
        collect.put(s, new Integer(value + 1));
      } else {
        collect.put(s, new Integer(1));
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
    collect  = new HashMap<k, Integer>();
  }

  /**
   * Output port that emits a map of integer values.
   */
  public final transient DefaultOutputPort<Map<k, Integer>> outport = new DefaultOutputPort<Map<k, Integer>>();

        /**
   * Output dimensions port that emits a map of &lt;string,object&gt; values.
   */
  public final transient DefaultOutputPort<Map<String, Object>> dimensionOut = new DefaultOutputPort<Map<String, Object>>();

  /**
   * Output total port that emits a map of &lt;string,integer&gt; count values.
   */
  public final transient DefaultOutputPort<Map<String, Integer>> total = new DefaultOutputPort<Map<String, Integer>>();

  @Override
  public void endWindow()
  {
    outport.emit(collect);
    long timestamp = new Date().getTime();
    int allcount = 0;
    for (Map.Entry<k, Integer> entry : collect.entrySet()) {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("timestamp", timestamp);
      map.put("item", entry.getKey());
      map.put("view", entry.getValue());
      dimensionOut.emit(map);
      allcount += entry.getValue();
    }
    Map<String, Integer> map = new HashMap<String, Integer>();
    map.put("total", new Integer(allcount));
    total.emit(map);
    collect = null;
    collect  = new HashMap<k, Integer>();
  }
}
