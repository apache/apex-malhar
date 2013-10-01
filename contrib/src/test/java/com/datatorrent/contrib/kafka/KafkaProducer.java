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

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
/**
 * A kafka producer for testing
 */
public class KafkaProducer implements Runnable
{
//  private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
  private final kafka.javaapi.producer.Producer<String, String> producer;
  private final String topic;
  private int sendCount = 20;

  public int getSendCount()
  {
    return sendCount;
  }

  public void setSendCount(int sendCount)
  {
    this.sendCount = sendCount;
  }

  private ProducerConfig createProducerConfig()
  {
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", "localhost:9092");
    props.setProperty("producer.type", "async");
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    
    return new ProducerConfig(props);
  }

  public KafkaProducer(String topic)
  {
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new Producer<String, String>(createProducerConfig());
    this.topic = topic;
  }

  @Override
  public void run()
  {
    // Create dummy message
    int messageNo = 1;
    while (messageNo <= sendCount) {
      String messageStr = "Message_" + messageNo;
      producer.send(new KeyedMessage<String, String>(topic, messageStr));
      messageNo++;
     // logger.debug(String.format("Producing %s", messageStr));
    }
    // produce the end tuple to let the test input operator know it's done produce messages
    producer.send(new KeyedMessage<String, String>(topic, KafkaOperatorTestBase.END_TUPLE));
  }

  public void close()
  {
    producer.close();
  }
} // End of KafkaProducer
