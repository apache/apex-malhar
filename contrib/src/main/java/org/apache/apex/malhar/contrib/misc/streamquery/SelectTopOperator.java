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
package org.apache.apex.malhar.contrib.misc.streamquery;

import java.util.ArrayList;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * An implementation of Operator that provides sql top select query semantic on live data stream. <br>
 * <p>
 * Stream rows passing condition are emitted on output port stream. <br>
 * <br>
 * <b>StateFull : NO,</b> all row data is processed in current time window. <br>
 * <b>Partitions : Yes, </b> No Input dependency among input rows. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b> inport : </b> Input hash map(row) port, expects
 * HashMap&lt;String,Object&gt;<<br>
 * <b> outport : </b> Output hash map(row) port, emits
 * HashMap&lt;String,Object&gt;<br>
 * <br>
 * <b> Properties : <b> <br>
 * <b> topValue : </b> top values count. <br>
 * <b> isPercentage : </b> top values count is percentage flag.
 * <br>
 * @displayName Select Top
 * @category Stream Manipulators
 * @tags sql select, sql top operator
 *  @since 0.3.4
 *  @deprecated
 */
@Deprecated
public class SelectTopOperator implements Operator
{
  private ArrayList<Map<String, Object>> list;
  private int topValue = 1;
  private boolean isPercentage = false;

  /**
   * Input port that takes a map of &lt;string,object&gt;.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      list.add(tuple);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void beginWindow(long windowId)
  {
    list = new ArrayList<Map<String, Object>>();
  }

  @Override
  public void endWindow()
  {
    int numEmits = topValue;
    if (isPercentage) {
      numEmits = list.size() * (topValue / 100);
    }
    for (int i = 0; (i < numEmits) && (i < list.size()); i++) {
      outport.emit(list.get(i));
    }
  }

  public int getTopValue()
  {
    return topValue;
  }

  public void setTopValue(int topValue) throws Exception
  {
    if (topValue <= 0) {
      throw new Exception("Top value must be positive number.");
    }
    this.topValue = topValue;
  }

  public boolean isPercentage()
  {
    return isPercentage;
  }

  public void setPercentage(boolean isPercentage)
  {
    this.isPercentage = isPercentage;
  }

  /**
   * Output port that emits a map of &lt;string,object&gt;.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport =  new DefaultOutputPort<Map<String, Object>>();
}
