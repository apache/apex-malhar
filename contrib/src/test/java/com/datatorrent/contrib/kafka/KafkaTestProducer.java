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

import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
/**
 * A kafka producer for testing
 */
public class KafkaTestProducer implements Runnable
{
//  private static final Logger logger = LoggerFactory.getLogger(KafkaTestProducer.class);
  private final kafka.javaapi.producer.Producer<String, String> producer;
  private final String topic;
  private int sendCount = 20;
  // to generate a random int as a key for partition
  private final Random rand = new Random();
  private boolean hasPartition = false;
  private List<String> messages;

  public int getSendCount()
  {
    return sendCount;
  }

  public void setSendCount(int sendCount)
  {
    this.sendCount = sendCount;
  }

  public void setMessages(List<String> messages) {
    this.messages = messages;
  }

  private ProducerConfig createProducerConfig()
  {
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
//    props.put("metadata.broker.list", "localhost:"+KafkaOperatorTestBase.TEST_KAFKA_BROKER1_PORT );
    if(hasPartition){
      props.put("metadata.broker.list", "localhost:"+KafkaOperatorTestBase.TEST_KAFKA_BROKER1_PORT + ",localhost:" + KafkaOperatorTestBase.TEST_KAFKA_BROKER2_PORT);
      props.setProperty("partitioner.class", KafkaTestPartitioner.class.getCanonicalName());
    } else {
      props.put("metadata.broker.list", "localhost:"+KafkaOperatorTestBase.TEST_KAFKA_BROKER1_PORT );
    }
    props.setProperty("topic.metadata.refresh.interval.ms", "20000");

    props.setProperty("producer.type", "async");

    return new ProducerConfig(props);
  }

  public KafkaTestProducer(String topic)
  {
    this(topic, false);
  }

  public KafkaTestProducer(String topic, boolean hasPartition)
  {
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    this.topic = topic;
    this.hasPartition = hasPartition;
    producer = new Producer<String, String>(createProducerConfig());
  }

  private void generateMessages()
  {
    // Create dummy message
    int messageNo = 1;
    while (messageNo <= sendCount) {
      String messageStr = "Message_" + messageNo;
      int k = rand.nextInt(100);
      producer.send(new KeyedMessage<String, String>(topic, "" + k, messageStr));
      messageNo++;
     // logger.debug(String.format("Producing %s", messageStr));
    }
    // produce the end tuple to let the test input operator know it's done produce messages
    producer.send(new KeyedMessage<String, String>(topic, "" + 0, KafkaOperatorTestBase.END_TUPLE));
    if(hasPartition){
      // Send end_tuple to other partition if it exist
      producer.send(new KeyedMessage<String, String>(topic, "" + 1, KafkaOperatorTestBase.END_TUPLE));
    }
  }

  @Override
  public void run()
  {
    if (messages == null) {
      generateMessages();
    } else {
      for (String msg : messages) {
        producer.send(new KeyedMessage<String, String>(topic, "", msg));
      }
    }
  }

  public void close()
  {
    producer.close();
  }
} // End of KafkaTestProducer
