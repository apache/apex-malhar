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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.contrib.kafka;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KafkaProducer implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
  private final kafka.javaapi.producer.Producer<Integer, String> producer;
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

  private ProducerConfig createProducerConfig(boolean isSimple)
  {
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    if (isSimple) {
      props.setProperty("broker.list", "1:localhost:2182");
      props.setProperty("producer.type", "async");
      props.setProperty("queue.time", "2000");
      props.setProperty("queue.size", "100");
      props.setProperty("batch.size", "10");
    }
    else {
      props.setProperty("zk.connect", "localhost:2182");
    }

    return new ProducerConfig(props);
  }

  public KafkaProducer(String topic, boolean isSimple)
  {
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new Producer<Integer, String>(createProducerConfig(isSimple));
    this.topic = topic;
  }

  @Override
  public void run()
  {
    // Create dummy message
    int messageNo = 1;
    while (messageNo <= sendCount) {
      String messageStr = "Message_" + messageNo;
      producer.send(new ProducerData<Integer, String>(topic, messageStr));
      messageNo++;
     // logger.debug(String.format("Producing %s", messageStr));
    }
  }

  public void close()
  {
    producer.close();
  }
} // End of KafkaProducer
