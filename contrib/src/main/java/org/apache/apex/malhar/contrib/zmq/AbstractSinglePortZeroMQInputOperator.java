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

import com.datatorrent.api.DefaultOutputPort;

/**
 * This is the base implementation of a single port ZeroMQ input operator.&nbsp;
 * This operator will behave like a subscriber that issues requests.&nbsp;
 * Subclasses should implement the methods which convert ZeroMQ messages into tuples.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have one output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method getTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractSinglePortZeroMQInputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>400 thousand K,V pairs/s</td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Operator can emit about 400 thousand unique (k,v immutable pairs) tuples/sec as ZeroMQ DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * </p>
 * @displayName Abstract Single Port ZeroMQ Input
 * @category Messaging
 * @tags output operator
 * @since 0.3.2
 */
public abstract class AbstractSinglePortZeroMQInputOperator<T> extends AbstractBaseZeroMQInputOperator
{
  /**
   * This is the ouput port on which tuples extracted from ZeroMQ are emitted.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Any concrete class derived from AbstractSinglePortZeroMQInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   * It converts a byte message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param message
   */
  public abstract T getTuple(byte[] message);

  @Override
  public void emitTuple(byte[] message)
  {
    outputPort.emit(getTuple(message));
  }
}
