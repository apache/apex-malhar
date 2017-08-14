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
package org.apache.apex.benchmark.kafka;

import org.apache.apex.malhar.contrib.kafka.AbstractKafkaInputOperator;

import com.datatorrent.api.DefaultOutputPort;

import kafka.message.Message;

/**
 * This operator emits one constant message for each kafka message received.&nbsp;
 * So we can track the throughput by messages emitted per second in the stram platform.
 * <p></p>
 * @displayName Benchmark Partitionable Kafka Input
 * @category Messaging
 * @tags input operator
 *
 * @since 0.9.3
 */
public class BenchmarkKafkaInputOperator extends AbstractKafkaInputOperator
{
  /**
   * The output port on which messages are emitted.
   */
  public transient DefaultOutputPort<String> oport = new DefaultOutputPort<String>();

  @Override
  protected void emitTuple(Message message)
  {
    oport.emit("Received");
  }

}
