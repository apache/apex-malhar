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

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.collect.Lists;

/**
 * A kafka producer for testing
 */
public class KafkaTestProducer implements Runnable
{
  //  private static final Logger logger = LoggerFactory.getLogger(KafkaTestProducer.class);
  private final Producer<String, String> producer;
  private final Producer<String, String> producer1;
  private final String topic;
  private int sendCount = 20;
  // to generate a random int as a key for partition
  private final Random rand = new Random();
  private boolean hasPartition = false;
  private boolean hasMultiCluster = false;
  private List<String> messages;

  // http://kafka.apache.org/documentation.html#producerconfigs
  private String ackType = "1";

  public int getSendCount()
  {
    return sendCount;
  }

  public void setSendCount(int sendCount)
  {
    this.sendCount = sendCount;
  }

  public void setMessages(List<String> messages)
  {
    this.messages = messages;
  }

  private Properties createProducerConfig(int cid)
  {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, KafkaTestPartitioner.class.getName());
    String brokerList = "localhost:" + KafkaOperatorTestBase.TEST_KAFKA_BROKER_PORT[cid];
    brokerList += hasPartition ? (",localhost:" + KafkaOperatorTestBase.TEST_KAFKA_BROKER_PORT[cid]) : "";
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.METADATA_MAX_AGE_CONFIG, "20000");
    props.setProperty(ProducerConfig.ACKS_CONFIG, getAckType());
    props.setProperty(ProducerConfig.RETRIES_CONFIG, "1");
    props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

    return props;
  }

  public KafkaTestProducer(String topic)
  {
    this(topic, false);
  }

  public KafkaTestProducer(String topic, boolean hasPartition, boolean hasMultiCluster)
  {
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    this.topic = topic;
    this.hasPartition = hasPartition;
    this.hasMultiCluster = hasMultiCluster;
    producer = new KafkaProducer<>(createProducerConfig(0));
    if (hasMultiCluster) {
      producer1 = new KafkaProducer<>(createProducerConfig(1));
    } else {
      producer1 = null;
    }
  }

  public KafkaTestProducer(String topic, boolean hasPartition)
  {
    this(topic, hasPartition, false);
  }

  private transient List<Future<RecordMetadata>> sendTasks = Lists.newArrayList();

  private void generateMessages()
  {
    // Create dummy message
    int messageNo = 1;
    while (messageNo <= sendCount) {
      String messageStr = "_" + messageNo++;
      int k = rand.nextInt(100);
      sendTasks.add(producer.send(new ProducerRecord<>(topic, "" + k, "c1" + messageStr)));
      if (hasMultiCluster && messageNo <= sendCount) {
        messageStr = "_" + messageNo++;
        sendTasks.add(producer1.send(new ProducerRecord<>(topic, "" + k, "c2" + messageStr)));
      }
      // logger.debug(String.format("Producing %s", messageStr));
    }
    // produce the end tuple to let the test input operator know it's done produce messages
    for (int i = 0; i < (hasPartition ? 2 : 1); ++i) {
      sendTasks.add(producer.send(new ProducerRecord<>(topic, "" + i, KafkaOperatorTestBase.END_TUPLE)));
      if (hasMultiCluster) {
        sendTasks.add(producer1.send(new ProducerRecord<>(topic, "" + i, KafkaOperatorTestBase.END_TUPLE)));
      }
    }
  }

  @Override
  public void run()
  {
    if (messages == null) {
      generateMessages();
    } else {
      for (String msg : messages) {
        sendTasks.add(producer.send(new ProducerRecord<>(topic, "", msg)));
      }
    }

    producer.flush();
    if (producer1 != null) {
      producer1.flush();
    }

    try {
      for (Future<RecordMetadata> task : sendTasks) {
        task.get(30, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    close();
  }

  public void close()
  {
    producer.close();
    if (producer1 != null) {
      producer1.close();
    }
  }

  public String getAckType()
  {
    return ackType;
  }

  public void setAckType(String ackType)
  {
    this.ackType = ackType;
  }
} // End of KafkaTestProducer
