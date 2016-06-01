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
package com.datatorrent.lib.stream;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * An implementation of BaseOperator that merges multiple streams with identical schema and emits the tuples
 * to the output port in order.
 *
 * <p>
 * This is a pass through operator<br>
 * <b>Ports</b>:<br>
 * <b>data1</b>: expects K<br>
 * <b>data2</b>: expects K<br>
 * <b>out</b>: emits K<br>
 * <br>
 * @displayName Stream Merger
 * @category Stream Manipulators
 * @tags merge
 * @since 0.3.3
 */
@Stateless
public class StreamMerger<K> extends BaseOperator
{
  private int portCount = 0;
  private final transient ArrayList<DefaultInputPort<K>> ports = new ArrayList<>(portCount);

  public StreamMerger()
  {
  }

  public StreamMerger(int portCount)
  {
    this.portCount = portCount;
  }

  /**
   * Track the number of created streams so that the operator can be re-initialized properly in the case of
   * an operator restart.
   * @param context
   */
  @Override
  public void setup(Context.OperatorContext context)
  {
    for (int i = 0; i < portCount; i++) {
      addStream();
    }
  }

  public StreamMerger<K> addStream() {
    ports.add(new DefaultInputPort<K>()
    {
      @Override
      public void process(K tuple)
      {
        out.emit(tuple);
      }
    });

    return this;
  }

  public DefaultInputPort<K> getPort(int idx) {
    return ports.get(idx);
  }

  public int getPortCount()
  {
    return portCount;
  }

  public void setPortCount(int portCount)
  {
    this.portCount = portCount;
  }

  /**
   * Output port.
   */
  public final transient DefaultOutputPort<K> out = new DefaultOutputPort<K>();
}
