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
package org.apache.apex.malhar.contrib.zmq;

/**
 * Output adapter operator with a single input port which consumes byte array and emits the same to the ZeroMQ port.
 * <p></p>
 *
 * @displayName Single Port Zero MQ output operator
 * @category Messaging
 * @tags input operator, string
 *
 * @since 3.0.0
 */

public class ZeroMQOutputOperator extends AbstractSinglePortZeroMQOutputOperator<byte[]>
{
  @Override
  public void processTuple(byte[] tuple)
  {
    publisher.send(tuple, 0);
  }
}
