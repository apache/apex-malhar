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
package org.apache.apex.malhar.contrib.kafka;

import com.datatorrent.api.DefaultOutputPort;

import kafka.message.Message;


/**
 * This is the base implementation of the Kafka input operator with a single output port,
 * which consumes data from the Kafka message bus.&nbsp;
 * Subclasses should implement the methods which convert Kafka messages to tuples.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Have only one output port<br>
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
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 *
 * @displayName Abstract Kafka Single Port Input
 * @category Messaging
 * @tags input operator
 *
 * @since 0.3.2
 */
public abstract class AbstractKafkaSinglePortInputOperator<T> extends AbstractKafkaInputOperator<KafkaConsumer>
{
  /**
   * This output port emits tuples extracted from Kafka messages.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Any concrete class derived from AbstractKafkaSinglePortInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   * It converts a ByteBuffer message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param msg
   */
  public abstract T getTuple(Message msg);

  /**
   * Implement abstract method.
   */
  @Override
  public void emitTuple(Message msg)
  {
    outputPort.emit(getTuple(msg));
  }
}
