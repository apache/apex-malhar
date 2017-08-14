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

import org.apache.apex.malhar.lib.util.BaseKeyOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;

/**
 * An implementation of BaseKeyValueOperator that duplicates an input stream as is into two output streams.
 * <p>
 * Duplication is needed to allow separation of listeners into two streams with different properties (for example
 * inline vs in-rack)<p>
 * This is a pass through operator<br>
 * <br>
 * <b>Port Interface</b><br>
 * <b>data</b>: expects &lt;K&gt;<br>
 * <b>out1</b>: emits &lt;K&gt;<br>
 * <b>out2</b>: emits &lt;K&gt;<br>
 * <br>
 * @displayName Stream duplicator
 * @category Stream Manipulators
 * @tags duplicate
 * @since 0.3.2
 */
@Stateless
public class StreamDuplicater<K> extends BaseKeyOperator<K>
{
  /**
   * Input data port.
   */
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Emits tuple on both streams
     */
    @Override
    public void process(K tuple)
    {
      out1.emit(cloneKey(tuple));
      out2.emit(cloneKey(tuple));
    }
  };

  /**
   * Output port 1 that emits duplicate of input stream.
   */
  public final transient DefaultOutputPort<K> out1 = new DefaultOutputPort<K>();

  /**
   * Output port 2 that emits duplicate of input stream.
   */
  public final transient DefaultOutputPort<K> out2 = new DefaultOutputPort<K>();
}
