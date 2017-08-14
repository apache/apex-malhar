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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * An implementation of BaseOperator that merges two streams with identical schema and emits the tuples to the output port in order.
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
  /**
   * Data input port 1.
   */
  public final transient DefaultInputPort<K> data1 = new DefaultInputPort<K>()
  {
    /**
     * Emits to port "out"
     */
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

  /**
   * Data input port 2.
   */
  public final transient DefaultInputPort<K> data2 = new DefaultInputPort<K>()
  {
    /**
     * Emits to port "out"
     */
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

  /**
   * Output port.
   */
  public final transient DefaultOutputPort<K> out = new DefaultOutputPort<K>();
}
