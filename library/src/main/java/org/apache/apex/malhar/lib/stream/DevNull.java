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
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * An implementation of BaseOperator that terminates a stream and does not affect the tuple.
 * <p>
 * Useful if you want to have a stream for monitoring purpose etc. In future STRAM may simply support a
 * virtual stream and make this operator redundant<br>
 * <br>
 * <b>Port</b>:<br>
 * <b>data</b>: expects K<br>
 * <br>
 * @displayName Dev Null
 * @category Stream Manipulators
 * @tags debug, terminate
 * @since 0.3.3
 */
@Stateless
public class DevNull<K> extends BaseOperator
{
  /**
   * Input any data type port.
   */
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    @Override
    public void process(K tuple)
    {
      // Does nothing; allows a stream to terminate and therefore be debugged
    }
  };
}
