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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

/**
 * A kafka consumer for testing
 */
public class KafkaTestConsumer implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
  private final transient ConsumerConnector consumer;
  protected static final int BUFFER_SIZE_DEFAULT = 1024 * 1024; // 1M
  // Config parameters that user can set.
  private final int bufferSize = BUFFER_SIZE_DEFAULT;
  public transient ArrayBlockingQueue<Message> holdingBuffer = new ArrayBlockingQueue<Message>(bufferSize);
  private final String topic;
  private String zkaddress = "localhost:2182";
  private boolean isAlive = true;
  private int receiveCount = 0;
  // A latch object to notify the waiting thread that it's done consuming the message
  private CountDownLatch latch;

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

  public KafkaTestConsumer(String topic)
  {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    this.topic = topic;
  }

  public KafkaTestConsumer(String topic, String zkaddress)
  {
    this.zkaddress = zkaddress;
    this.topic = topic;
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
  }

  private ConsumerConfig createConsumerConfig()
  {
    Properties props = new Properties();
    props.setProperty("zookeeper.connect", zkaddress);
    props.setProperty("group.id", "group1");
    props.put("auto.offset.reset", "smallest");
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
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    logger.debug("Inside consumer::run receiveCount= {}", receiveCount);
    while (it.hasNext() & isAlive) {
      Message msg = new Message(it.next().message());
      if (latch != null) {
        latch.countDown();
      }
      if (getMessage(msg).equals(KafkaOperatorTestBase.END_TUPLE)) {
        break;
      }
      holdingBuffer.add(msg);
      receiveCount++;
      logger.debug("Consuming {}, receiveCount= {}", getMessage(msg), receiveCount);
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        break;
      }
    }
    logger.debug("DONE consuming");
  }

  public void close()
  {
    holdingBuffer.clear();
    consumer.shutdown();
  }

  public void setLatch(CountDownLatch latch)
  {
    this.latch = latch;
  }
} // End of KafkaTestConsumer
