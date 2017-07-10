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
package org.apache.apex.malhar.kafka;

import java.util.Properties;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.datatorrent.api.DefaultOutputPort;

/**
 * This is just an example of single port operator emits only byte array messages
 * The key and cluster information are ignored
 * This class emit the value to the single output port
 *
 */
@InterfaceStability.Evolving
public class KafkaSinglePortInputOperator extends AbstractKafkaInputOperator
{
  /**
   * Create the consumer for 0.10.* version of Kafka
   * @param prop consumer properties
   * @return consumer
   */
  @Override
  public AbstractKafkaConsumer createConsumer(Properties prop)
  {
    return new KafkaConsumer010<byte[], byte[]>(prop);
  }

  /**
   * This output port emits tuples extracted from Kafka messages.
   */
  public final transient DefaultOutputPort<byte[]> outputPort = new DefaultOutputPort<>();

  @Override
  protected void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message)
  {
    outputPort.emit(message.value());
  }

}
