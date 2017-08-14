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
package org.apache.apex.malhar.lib.stream;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.Unifier;

/**
 * An implementation of Operator and Unifier that counts the number of tuples delivered to it in each window and emits the count.
 * <p>
 * This is a pass through operator<br>
 * <br>
 * <b>StateFull : Yes</b>, tuples are counted over application window. <br>
 * <b>Partitions : Yes</b>, count is unified at output port. <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Object<br>
 * <b>output</b>: emits Integer<br>
 * <br>
 * @displayName Counter
 * @category Stream Manipulators
 * @tags integer, object, count
 * @since 0.3.3
 */
public class Counter implements Operator, Unifier<Integer>
{
  /**
   * Input port that takes objects to be counted in each window.
   */
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      Counter.this.count++;
    }

  };

  /**
   * Output port that takes emits count in each window.
   */
  public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>()
  {
    @Override
    public Unifier<Integer> getUnifier()
    {
      return Counter.this;
    }

  };

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void process(Integer tuple)
  {
    count += tuple;
  }

  @Override
  public void endWindow()
  {
    output.emit(count);
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  private transient int count;
}
