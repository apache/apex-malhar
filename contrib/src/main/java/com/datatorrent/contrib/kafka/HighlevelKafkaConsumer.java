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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.constraints.Min;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

/**
 * High level kafka consumer used for kafka input operator
 */
public class HighlevelKafkaConsumer extends KafkaConsumer
{
  private Properties consumerConfig = null;

  private transient ConsumerConnector standardConsumer = null;
  
  /**
   * Carefully set this number to how many partition use setup for your upstream Kafka topic
   * Otherwise it might slow down the performance
   * There must be at least 1 stream
   */
  @Min(value = 1)
  private int numStream = 1;
  
  public HighlevelKafkaConsumer()
  {
  }
  
  public HighlevelKafkaConsumer(Properties consumerConfig)
  {
    super();
    this.consumerConfig = consumerConfig;
  }
  
  @Override
  public void create()
  {
    super.create();
    standardConsumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerConfig));
  }

  @Override
  public void start()
  {
    super.start();
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(numStream)); // take care int, how to handle multiple topics
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = standardConsumer.createMessageStreams(topicCountMap);
    
    // start $numStream anonymous threads to consume the data 
    ExecutorService executor = Executors.newFixedThreadPool(numStream);
    for(final KafkaStream<byte[], byte[]> stream : consumerMap.get(topic)){
      executor.submit(new Runnable() {
        public void run() {
          ConsumerIterator<byte[], byte[]> itr = stream.iterator();
          while (itr.hasNext() && isAlive) {
            holdingBuffer.add(new Message(itr.next().message()));
          }
        }
      });
    }
  }

  @Override
  public void stop()
  {
    isAlive = false;
    standardConsumer.shutdown();
  }

  
  public void setConsumerConfig(Properties consumerConfig)
  {
    this.consumerConfig = consumerConfig;
  }
}
