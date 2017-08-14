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
package org.apache.apex.benchmark.stream;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 *
 * Integer input operator which emits Integer tuples only.
 * This operator is benchmarked to emit more than 2 million tuples/sec on cluster node.
 *
 * @since 2.0.0
 */
public class IntegerOperator implements InputOperator
{
  /**
   * Output port which emits integer.
   */
  public final transient DefaultOutputPort<Integer> integer_data = new DefaultOutputPort<Integer>();

  @Override
  public void emitTuples()
  {
    Integer i = 21;
    for (int j = 0; j < 1000; j++) {
      integer_data.emit(i);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    //throw new UnsupportedOperationException("Not supported yet.");
    // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void endWindow()
  {
    //throw new UnsupportedOperationException("Not supported yet.");
    // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setup(OperatorContext context)
  {
    //throw new UnsupportedOperationException("Not supported yet.");
    // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void teardown()
  {
    //throw new UnsupportedOperationException("Not supported yet.");
    // To change body of generated methods, choose Tools | Templates.
  }

}
