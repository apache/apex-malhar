/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kafka.benchmark;

import kafka.message.Message;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.kafka.AbstractPartitionableKafkaInputOperator;

/**
 * This operator emits one constant message for each kafka message received.&nbsp;
 * So we can track the throughput by messages emitted per second in the stram platform.
 * <p></p>
 * @displayName Benchmark Partitionable Kafka Input
 * @category messaging
 * @tags input operator
 *
 * @since 0.9.3
 */
public class BenchmarkPartitionableKafkaInputOperator extends AbstractPartitionableKafkaInputOperator
{
  /**
   * The output port on which messages are emitted.
   */
  @InputPortFieldAnnotation(name = "KafkaOutput")
  public transient DefaultOutputPort<String>  oport = new DefaultOutputPort<String>();

  @Override
  protected AbstractPartitionableKafkaInputOperator cloneOperator()
  {
    return new BenchmarkPartitionableKafkaInputOperator();
  }

  @Override
  protected void emitTuple(Message message)
  {
    oport.emit("Received");
  }

}
