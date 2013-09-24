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

import java.nio.ByteBuffer;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.message.Message;

/**
 * An template concrete class of AbstractKafkaSinglePortInputOperator for getting string input from Kafka message.
 */
public class KafkaSinglePortStringInputOperator extends AbstractKafkaSinglePortInputOperator<String>
{
  
  private Properties configProperties = null;

  @Override
  public ConsumerConfig createKafkaConsumerConfig()
  {
    return new ConsumerConfig(configProperties);
  }

  /**
   * Implement abstract method of AbstractActiveMQSinglePortInputOperator
   */
  @Override
  public String getTuple(Message message)
  {
    String data = "";
    try {
      ByteBuffer buffer = message.payload();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      data = new String(bytes);
      //logger.debug("Consuming {}", data);
    }
    catch (Exception ex) {
      return data;
    }
    return data;
  }

  public Properties getConfigProperties()
  {
    return configProperties;
  }

  public void setConfigProperties(Properties configProperties)
  {
    this.configProperties = configProperties;
  }
}
