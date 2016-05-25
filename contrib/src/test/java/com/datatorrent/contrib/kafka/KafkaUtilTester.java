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
package com.datatorrent.contrib.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.common.util.Pair;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaUtilTester extends KafkaOperatorTestBase
{
  public static final String topic = "UtilTestTopic";
  public static final String clientNamePrefix = "UtilTestClient";
  public static final int DATA_SIZE = 50;

  protected Producer<String, String> producer;
  private transient Set<String> brokerSet;

  public void beforeTest()
  {
    //Got exception when using multiple partition.
    //java.io.FileNotFoundException: target/kafka-server-data/1/1/replication-offset-checkpoint (No such file or directory)
    //hasMultiPartition = true;

    super.beforeTest();
    createTopic(topic);

    producer = new Producer<String, String>(createKafkaProducerConfig());
    getBrokerSet();

    sendData();
  }

  public void sendData()
  {
    for (int i = 0; i < DATA_SIZE; ++i) {
      producer.send(new KeyedMessage<String, String>(topic, null, "message " + i));
    }

    waitMills(1000);
  }

  @Test
  public void testReadMessagesAfterOffsetTo()
  {
    Map<Integer, Long> partitionToStartOffset = Maps.newHashMap();
    partitionToStartOffset.put(1, 0L);
    Map<Integer, List<Pair<byte[], byte[]>>> partitionToMessages = Maps.newHashMap();
    KafkaUtil.readMessagesAfterOffsetTo(clientNamePrefix, brokerSet, topic, partitionToStartOffset,
        partitionToMessages);
    final int dataSize = partitionToMessages.entrySet().iterator().next().getValue().size();
    Assert.assertTrue("data size is: " + dataSize, dataSize == DATA_SIZE);
  }

  public void waitMills(long millis)
  {
    try {
      Thread.sleep(millis);
    } catch (Exception e) {
      //ignore
    }
  }

  protected void createTopic(String topicName)
  {
    createTopic(0, topicName);
    if (hasMultiCluster) {
      createTopic(1, topicName);
    }
  }

  protected Properties getConfigProperties()
  {
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    //props.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    //props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", "localhost:9092");
    //props.setProperty("producer.type", "sync");
    props.setProperty("producer.type", "async");
    props.setProperty("queue.buffering.max.ms", "10");
    props.setProperty("queue.buffering.max.messages", "10");
    props.setProperty("batch.num.messages", "5");

    return props;
  }

  protected ProducerConfig createKafkaProducerConfig()
  {
    return new ProducerConfig(getConfigProperties());
  }

  protected Set<String> getBrokerSet()
  {
    if (brokerSet == null) {
      brokerSet = Sets.newHashSet((String)getConfigProperties().get(KafkaMetadataUtil.PRODUCER_PROP_BROKERLIST));
    }
    return brokerSet;
  }
}
