/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KafkaConsumer implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
  private final transient ConsumerConnector consumer;
  protected static final int BUFFER_SIZE_DEFAULT = 1024 * 1024; // 1M
  // Config parameters that user can set.
  private int bufferSize = BUFFER_SIZE_DEFAULT;
  public transient ArrayBlockingQueue<Message> holdingBuffer = new ArrayBlockingQueue<Message>(bufferSize);;
  private final String topic;
  private boolean isAlive = true;
  private int receiveCount = 0;

  public int getReceiveCount()
  {
    return receiveCount;
  }

  public void setReceiveCount(int receiveCount)
  {
    this.receiveCount = receiveCount;
  }

  public void setIsAlive(boolean isAlive)
  {
    this.isAlive = isAlive;
  }

  public KafkaConsumer(String topic)
  {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    this.topic = topic;
  }

  private ConsumerConfig createConsumerConfig()
  {
    Properties props = new Properties();
    props.setProperty("zk.connect", "localhost:2182");
    props.setProperty("groupid", "group1");
    return new ConsumerConfig(props);
  }

  public String getMessage(Message message)
  {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }

  @Override
  public void run()
  {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<Message> stream = consumerMap.get(topic).get(0);
    ConsumerIterator<Message> it = stream.iterator();
    logger.debug("Inside consumer::run receiveCount= {}", receiveCount);
    while (it.hasNext() & isAlive) {
      Message msg = it.next().message();
      holdingBuffer.add(msg);
      receiveCount++;
      logger.debug("Consuming {}, receiveCount= {}", getMessage(msg), receiveCount);
    }
    logger.debug("DONE consuming");
  }

  public void close()
  {
    holdingBuffer.clear();
    consumer.shutdown();
  }
} // End of KafkaConsumer
