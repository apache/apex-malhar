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
package com.datatorrent.contrib.netflow;

import java.util.Properties;

import javax.validation.constraints.NotNull;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * KafkaOutput
 *
 * @since 0.9.2
 */
public class KafkaOutput<V>
{

  private transient kafka.javaapi.producer.Producer<String, V> producer; // K is key partitioner, V is value type
  @NotNull
  private String topic = "test";
  
  Properties configProperties;

  public Producer<String, V> getProducer()
  {
    return producer;
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public KafkaOutput(Properties configProperties)
  {
    this.configProperties = configProperties;
    producer = new Producer<String, V>(new ProducerConfig(configProperties));
  }

  public Properties getConfigProperties()
  {
    return configProperties;
  }

  public void setConfigProperties(Properties configProperties)
  {
    this.configProperties = configProperties;
  }
  public void close(){
    producer.close();
  }

  public void process(V tuple)
  {
    producer.send(new KeyedMessage<String, V>(topic, tuple));
  }

  
}
