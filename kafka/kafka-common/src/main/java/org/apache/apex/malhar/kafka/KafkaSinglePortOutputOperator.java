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

import org.apache.kafka.clients.producer.ProducerRecord;

import com.datatorrent.api.DefaultInputPort;

/**
 * Kafka output operator with single input port (inputPort).
 * It supports atleast once processing guarantees
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class KafkaSinglePortOutputOperator<K, V> extends AbstractKafkaOutputOperator
{
  /**
   * This input port receives tuples that will be written out to Kafka.
   */
  public final transient DefaultInputPort<V> inputPort = new DefaultInputPort<V>()
  {
    @Override
    public void process(V tuple)
    {
      getProducer().send(new ProducerRecord<K, V>(getTopic(), tuple));
    }
  };
}
