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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.common.util.Pair;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

public class KafkaUtil
{
  private static Logger logger = LoggerFactory.getLogger(KafkaUtil.class);
  public static final int DEFAULT_TIMEOUT = 200;
  public static final int DEFAULT_BUFFER_SIZE = 64 * 10240;
  public static final int DEFAULT_FETCH_SIZE = 200;

  /**
   * read last message ( the start offset send from partitionToOffset ) of all
   * partition to partitionToMessages
   * 
   * @param clientNamePrefix
   * @param brokerSet
   * @param topic
   * @param partitionToStartOffset
   * @param partitionToMessages
   */
  public static void readMessagesAfterOffsetTo(String clientNamePrefix, Set<String> brokerSet, String topic,
      Map<Integer, Long> partitionToStartOffset, Map<Integer, List<Pair<byte[], byte[]>>> partitionToMessages)
  {
    // read last received kafka message
    TopicMetadata tm = KafkaMetadataUtil.getTopicMetadata(brokerSet, topic);

    if (tm == null) {
      throw new RuntimeException("Failed to retrieve topic metadata");
    }

    for (PartitionMetadata pm : tm.partitionsMetadata()) {
      SimpleConsumer consumer = null;
      try {
        List<Pair<byte[], byte[]>> messagesOfPartition = partitionToMessages.get(pm.partitionId());
        if (messagesOfPartition == null) {
          messagesOfPartition = Lists.newArrayList();
          partitionToMessages.put(pm.partitionId(), messagesOfPartition);
        }

        long startOffset = partitionToStartOffset.get(pm.partitionId()) == null ? 0
            : partitionToStartOffset.get(pm.partitionId());
        final String clientName = KafkaMetadataUtil.getClientName(clientNamePrefix, tm.topic(), pm.partitionId());
        consumer = createSimpleConsumer(clientName, tm.topic(), pm);

        //the returned lastOffset is the offset which haven't written data to.
        long lastOffset = KafkaMetadataUtil.getLastOffset(consumer, tm.topic(), pm.partitionId(),
            kafka.api.OffsetRequest.LatestTime(), clientName);
        logger.debug("lastOffset = {}", lastOffset);
        if (lastOffset <= 0) {
          continue;
        }

        readMessagesBetween(consumer, clientName, topic, pm.partitionId(), startOffset, lastOffset - 1,
            messagesOfPartition);
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }
  }

  public static void readMessagesBetween(String clientNamePrefix, Set<String> brokerSet, String topic, int partitionId,
      long startOffset, long endOffset, List<Pair<byte[], byte[]>> messages)
  {
    Map<Integer, SimpleConsumer> consumers = createSimpleConsumers(clientNamePrefix, brokerSet, topic);
    if (consumers == null) {
      throw new RuntimeException("Can't find any consumer.");
    }

    SimpleConsumer consumer = consumers.get(partitionId);
    if (consumer == null) {
      throw new IllegalArgumentException("No consumer for partition: " + partitionId);
    }

    readMessagesBetween(consumer, KafkaMetadataUtil.getClientName(clientNamePrefix, topic, partitionId), topic,
        partitionId, startOffset, endOffset, messages);
  }

  /**
   * get A map of partition id to SimpleConsumer
   * 
   * @param clientNamePrefix
   * @param brokerSet
   * @param topic
   * @return A map of partition id to SimpleConsumer
   */
  public static Map<Integer, SimpleConsumer> createSimpleConsumers(String clientNamePrefix, Set<String> brokerSet,
      String topic)
  {
    return createSimpleConsumers(clientNamePrefix, brokerSet, topic, DEFAULT_TIMEOUT);
  }

  /**
   * get A map of partition id to SimpleConsumer
   * 
   * @param clientNamePrefix
   * @param brokerSet
   * @param topic
   * @param timeOut
   * @return A map of partition id to SimpleConsumer
   */
  public static Map<Integer, SimpleConsumer> createSimpleConsumers(String clientNamePrefix, Set<String> brokerSet,
      String topic, int timeOut)
  {
    if (clientNamePrefix == null || clientNamePrefix.isEmpty() || brokerSet == null || brokerSet.isEmpty()
        || topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException(
          "clientNamePrefix = " + clientNamePrefix + ", brokerSet = " + brokerSet + ", topic = " + topic);
    }

    TopicMetadata tm = KafkaMetadataUtil.getTopicMetadata(brokerSet, topic);

    if (tm == null) {
      throw new RuntimeException("Failed to retrieve topic metadata");
    }

    Map<Integer, SimpleConsumer> consumers = Maps.newHashMap();
    for (PartitionMetadata pm : tm.partitionsMetadata()) {
      String clientName = KafkaMetadataUtil.getClientName(clientNamePrefix, tm.topic(), pm.partitionId());
      consumers.put(pm.partitionId(), createSimpleConsumer(clientName, tm.topic(), pm));
    }
    return consumers;
  }

  public static void readMessagesBetween(SimpleConsumer consumer, String clientName, String topic, int partitionId,
      long startOffset, long endOffset, List<Pair<byte[], byte[]>> messages)
  {
    readMessagesBetween(consumer, clientName, topic, partitionId, startOffset, endOffset, messages, 1);
  }

  /**
   * read messages of a certain partition into messages
   * 
   * @param consumer
   * @param clientNamePrefix
   * @param topic
   * @param partitionId
   * @param startOffset
   *          inclusive
   * @param endOffset
   *          inclusive
   * @param messages
   * @param tryTimesOnEmptyMessage
   *          how many times should to try when response message is empty. <=0
   *          means try forever.
   */
  public static void readMessagesBetween(SimpleConsumer consumer, String clientName, String topic, int partitionId,
      long startOffset, long endOffset, List<Pair<byte[], byte[]>> messages, int tryTimesOnEmptyMessage)
  {
    if (startOffset < 0 || endOffset < 0 || endOffset < startOffset) {
      throw new IllegalArgumentException(
          "Both offset should not less than zero and endOffset should not less than startOffset. startOffset = "
              + startOffset + ", endoffset = " + endOffset);
    }

    int readSize = 0;
    int wantedSize = (int)(endOffset - startOffset + 1);

    int triedTimesOnEmptyMessage = 0;
    while (readSize < wantedSize
        && (tryTimesOnEmptyMessage <= 0 || triedTimesOnEmptyMessage < tryTimesOnEmptyMessage)) {
      logger.debug("startOffset = {}", startOffset);
      FetchRequest req = new FetchRequestBuilder().clientId(clientName)
          .addFetch(topic, partitionId, startOffset, DEFAULT_FETCH_SIZE).build();

      FetchResponse fetchResponse = consumer.fetch(req);
      if (fetchResponse.hasError()) {
        logger.error(
            "Error fetching data Offset Data the Broker. Reason: " + fetchResponse.errorCode(topic, partitionId));
        return;
      }

      triedTimesOnEmptyMessage++;
      ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, partitionId);
      for (MessageAndOffset messageAndOffset : messageSet) {
        long offset = messageAndOffset.offset();
        logger.debug("offset = " + offset);

        if (offset > endOffset || offset < startOffset) {
          continue;
        }
        triedTimesOnEmptyMessage = 0;
        startOffset = offset + 1;
        ++readSize;
        messages.add(kafkaMessageToPair(messageAndOffset.message()));
      }
    }
  }

  /**
   * read last message of each partition into lastMessages
   * 
   * @param clientNamePrefix
   * @param brokerSet
   * @param topic
   * @param lastMessages
   */
  public static void readLastMessages(String clientNamePrefix, Set<String> brokerSet, String topic,
      Map<Integer, Pair<byte[], byte[]>> lastMessages)
  {
    // read last received kafka message
    TopicMetadata tm = KafkaMetadataUtil.getTopicMetadata(brokerSet, topic);

    if (tm == null) {
      throw new RuntimeException("Failed to retrieve topic metadata");
    }

    for (PartitionMetadata pm : tm.partitionsMetadata()) {
      SimpleConsumer consumer = null;
      try {
        String clientName = KafkaMetadataUtil.getClientName(clientNamePrefix, tm.topic(), pm.partitionId());
        consumer = createSimpleConsumer(clientName, tm.topic(), pm);

        long readOffset = KafkaMetadataUtil.getLastOffset(consumer, tm.topic(), pm.partitionId(),
            kafka.api.OffsetRequest.LatestTime(), clientName);

        FetchRequest req = new FetchRequestBuilder().clientId(clientName)
            .addFetch(tm.topic(), pm.partitionId(), readOffset - 1, DEFAULT_FETCH_SIZE).build();

        FetchResponse fetchResponse = consumer.fetch(req);
        if (fetchResponse.hasError()) {
          logger.error("Error fetching data Offset Data the Broker. Reason: "
              + fetchResponse.errorCode(topic, pm.partitionId()));
          return;
        }

        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(tm.topic(), pm.partitionId())) {
          lastMessages.put(pm.partitionId(), kafkaMessageToPair(messageAndOffset.message()));
        }
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }

    }
  }

  public static Pair<byte[], byte[]> readLastMessage(String clientNamePrefix, Set<String> brokerSet, String topic,
      int partitionId)
  {
    // read last received kafka message
    TopicMetadata tm = KafkaMetadataUtil.getTopicMetadata(brokerSet, topic);

    if (tm == null) {
      throw new RuntimeException("Failed to retrieve topic metadata");
    }

    for (PartitionMetadata pm : tm.partitionsMetadata()) {
      SimpleConsumer consumer = null;
      try {
        if (pm.partitionId() != partitionId) {
          continue;
        }

        String clientName = KafkaMetadataUtil.getClientName(clientNamePrefix, tm.topic(), pm.partitionId());
        consumer = createSimpleConsumer(clientName, topic, pm);

        long readOffset = KafkaMetadataUtil.getLastOffset(consumer, topic, partitionId,
            kafka.api.OffsetRequest.LatestTime(), clientName);

        FetchRequest req = new FetchRequestBuilder().clientId(clientName)
            .addFetch(tm.topic(), pm.partitionId(), readOffset - 1, DEFAULT_FETCH_SIZE).build();

        FetchResponse fetchResponse = consumer.fetch(req);
        if (fetchResponse.hasError()) {
          logger.error("Error fetching data Offset Data the Broker. Reason: "
              + fetchResponse.errorCode(topic, pm.partitionId()));
          return null;
        }

        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionId)) {
          return kafkaMessageToPair(messageAndOffset.message());
        }
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }
    return null;
  }

  /**
   * convert Kafka message to pair
   * 
   * @param m
   * @return
   */
  public static Pair<byte[], byte[]> kafkaMessageToPair(Message m)
  {
    ByteBuffer payload = m.payload();
    ByteBuffer key = m.key();
    byte[] keyBytes = null;
    if (key != null) {
      keyBytes = new byte[key.limit()];
      key.get(keyBytes);
    }

    byte[] valueBytes = new byte[payload.limit()];
    payload.get(valueBytes);
    return new Pair<byte[], byte[]>(keyBytes, valueBytes);
  }

  public static SimpleConsumer createSimpleConsumer(String clientName, String topic, PartitionMetadata pm)
  {
    return createSimpleConsumer(clientName, topic, pm, DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE);
  }

  public static SimpleConsumer createSimpleConsumer(String clientName, String topic, PartitionMetadata pm, int timeout,
      int bufferSize)
  {
    String leadBroker = pm.leader().host();
    int port = pm.leader().port();
    return new SimpleConsumer(leadBroker, port, timeout, bufferSize, clientName);
  }
}
