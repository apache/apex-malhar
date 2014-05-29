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
package com.datatorrent.contrib.kafka;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.DefaultInputPort;
import kafka.producer.KeyedMessage;

/**
 * Kafka output adapter operator with only one input port, which produce data into Kafka message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Have only one input port<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method createKafkaProducerConfig() to setup producer configuration.<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 *
 * @since 0.3.2
 */
public class KafkaSinglePortOutputOperator<K, V> extends AbstractKafkaOutputOperator<K, V>
{

  /**
   * The single input port.
   */
  @InputPortFieldAnnotation(name = "KafkaInputPort")
  public final transient DefaultInputPort<V> inputPort = new DefaultInputPort<V>()
  {
    @Override
    public void process(V tuple)
    {
      // Send out single data
      getProducer().send(new KeyedMessage<K, V>(getTopic(), tuple));
      sendCount++;

      // TBD: Kafka also has an api to send out bunch of data in a list.
      // which is not yet supported here.

      //logger.debug("process message {}", tuple.toString());
    }
  };

}
