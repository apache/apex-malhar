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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import kafka.api.ConsumerMetadataRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.network.BlockingChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.kafka.KafkaPartition;
import com.datatorrent.contrib.kafka.OffsetManager;

public class KafkaTopicOffsetManager implements OffsetManager
{

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicOffsetManager.class);

  private transient List<TopicAndPartition> partitionList;

  private static final int DEFAULT_RETRY_COUNT = 3;

  private String consumerGroup;
  private String clientId;

  private String hostname;

  private int partitionCount;

  private String topic;

  private int port;

  private int retry;

  private long waitTimeout;

  /**
   * @return the port
   */
  public int getPort()
  {
    return port;
  }

  /**
   * @param port
   *          the port to set
   */
  public void setPort(int port)
  {
    this.port = port;
  }

  /**
   * @return the hostname
   */
  public String getHostname()
  {
    return hostname;
  }

  /**
   * @param hostname
   *          the hostname to set
   */
  public void setHostname(String hostname)
  {
    this.hostname = hostname;
  }

  /**
   * @return the partitionCount
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * @param partitionCount
   *          the partitionCount to set
   */
  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * @return the topic
   */
  public String getTopic()
  {
    return topic;
  }

  /**
   * @param topic
   *          the topic to set
   */
  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  private Map<TopicAndPartition, Long> offsetsMap;

  /**
   * @return the offsetsMap
   */
  public Map<TopicAndPartition, Long> getOffsetsMap()
  {
    return offsetsMap;
  }

  /**
   * @param offsetsMap
   *          the offsetsMap to set
   */
  public void setOffsetsMap(Map<TopicAndPartition, Long> offsetsMap)
  {
    this.offsetsMap = offsetsMap;
  }

  private int retryCount;

  /**
   * @return the retryCount
   */
  public int getRetryCount()
  {
    return retryCount;
  }

  /**
   * @param retryCount
   *          the retryCount to set
   */
  public void setRetryCount(int retryCount)
  {
    this.retryCount = retryCount;
  }

  private int correlationId;

  /**
   * @return the correlationId
   */
  public int getCorrelationId()
  {
    return correlationId;
  }

  /**
   * @param correlationId
   *          the correlationId to set
   */
  public void setCorrelationId(int correlationId)
  {
    this.correlationId = correlationId;
  }

  /**
   * @return the partitionList
   */
  public List<TopicAndPartition> getPartitionList()
  {
    return partitionList;
  }

  /**
   * @param partitionList
   *          the partitionList to set
   */
  public void setPartitionList(List<TopicAndPartition> partitionList)
  {
    this.partitionList = partitionList;
  }

  /**
   * @return the consumerGroup
   */
  public String getConsumerGroup()
  {
    return consumerGroup;
  }

  /**
   * @param consumerGroup
   *          the consumerGroup to set
   */
  public void setConsumerGroup(String consumerGroup)
  {
    this.consumerGroup = consumerGroup;
  }

  /**
   * @return the clientId
   */
  public String getClientId()
  {
    return clientId;
  }

  /**
   * @param clientId
   *          the clientId to set
   */
  public void setClientId(String clientId)
  {
    this.clientId = clientId;
  }

  private transient BlockingChannel channel;

  /**
   * @return the channel
   */
  public BlockingChannel getChannel()
  {
    return channel;
  }

  /**
   * @param channel
   *          the channel to set
   */
  public void setChannel(BlockingChannel channel)
  {
    this.channel = channel;
  }

  public KafkaTopicOffsetManager()
  {

  }

  public KafkaTopicOffsetManager(String hostname, int port, String consumerGroup, String clientID, String topic,
      int partitionCount)
  {

    partitionList = new LinkedList<TopicAndPartition>();

    offsetsMap = new HashMap<TopicAndPartition, Long>();

    setHostname(hostname);
    setConsumerGroup(consumerGroup);
    setClientId(clientID);
    setTopic(topic);
    setPartitionCount(partitionCount);
    setPort(port);
    setRetryCount(retryCount);

    getConnectionHandle(getRetry());
  }

  public void getConnectionHandle(int retry)
  {

    if (retry == 0) {
      setChannel(null);
      return;
    }

    int correlationID = 0;
    try {
      BlockingChannel channel = new BlockingChannel(getHostname(), getPort(), BlockingChannel.UseDefaultBufferSize(),
          BlockingChannel.UseDefaultBufferSize(), (int)getWaitTimeout() * 10);
      channel.connect();

      //TODO - do a get until you get no further data from the zk/topic

      for (int i = 0; i < getPartitionCount(); i++) {
        final TopicAndPartition tempPartition = new TopicAndPartition(getTopic(), i);
        partitionList.add(tempPartition);

      }

      channel.send(new ConsumerMetadataRequest(consumerGroup, (short)1, correlationID++, getClientId()));
      ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

      // Set the incremental correlation ID
      setCorrelationId(correlationID);

      if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
        Broker offsetManager = metadataResponse.coordinator();
        // if the coordinator is different, from the above channel's
        // host then reconnect
        channel.disconnect();
        channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
            BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(), (int)getWaitTimeout() * 10);

        channel.connect();
        setChannel(channel);
        LOG.info("Channel initialized");

      } else {
        setRetry(getRetry() - 1);
        getConnectionHandle(getRetry());
      }
    } catch (Exception e) {
      LOG.error("Not able to initialize the channel");
    }

  }

  /**
   * @return the retry
   */
  public int getRetry()
  {
    return retry;
  }

  /**
   * @param retry
   *          the retry to set
   */
  public void setRetry(int retry)
  {
    this.retry = retry;
  }

  /**
   * @return the waitTimeout
   */
  public long getWaitTimeout()
  {
    return waitTimeout;
  }

  /**
   * @param waitTimeout
   *          the waitTimeout to set
   */
  public void setWaitTimeout(long waitTimeout)
  {
    this.waitTimeout = waitTimeout;
  }

  public void fetchOffsets()
  {
    // version 1 and above fetch from Kafka, version 0 fetches from
    // ZooKeeper

    OffsetFetchRequest fetchRequest = new OffsetFetchRequest(getConsumerGroup(), getPartitionList(), (short)1,
        getCorrelationId(), getClientId());
    try {
      getChannel().send(fetchRequest.underlying());
      OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(getChannel().receive().buffer());

      ListIterator<TopicAndPartition> itr = getPartitionList().listIterator();

      //TODO - check and handle more error codes

      while (itr.hasNext()) {

        TopicAndPartition topicAndPartition = itr.next();
        OffsetMetadataAndError result = fetchResponse.offsets().get(topicAndPartition);
        short offsetFetchErrorCode = result.error();
        if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
          getChannel().disconnect();

        } else if (offsetFetchErrorCode == ErrorMapping.BrokerNotAvailableCode()) {

          for (int i = 0; i < getRetryCount(); i++) {
            // TODO- externalize this wait
            getChannel().wait(getWaitTimeout());
            result = fetchResponse.offsets().get(topicAndPartition);
            // Exit if there is no error
            if (result.error() == ErrorMapping.NoError()) {
              break;
            }
          }
          // retry the offset fetch (after backoff)
        } else {
          long retrievedOffset = result.offset();
          // TODO- use the metadata associated with an offset, its not required for offset maintenance as such
          String retrievedMetadata = result.metadata();
          getOffsetsMap().put(topicAndPartition, retrievedOffset);
        }

      }

    } catch (InterruptedException e) {
      LOG.error("Could not fetch offsets,Exception occurred" + e.getMessage() + "Closing channel");
      channel.disconnect();
    }

  }

  @Override
  public void updateOffsets(Map<KafkaPartition, Long> offsetsOfPartitions)
  {

    long now = System.currentTimeMillis();

    Map<TopicAndPartition, OffsetAndMetadata> offsets = new LinkedHashMap<TopicAndPartition, OffsetAndMetadata>();

    // Iterate over the updated offsets from the platform's partitions and
    // create the
    for (Entry<KafkaPartition, Long> entry : offsetsOfPartitions.entrySet()) {
      KafkaPartition key = entry.getKey();
      long value = entry.getValue();
      offsets.put(new TopicAndPartition(key.getTopic(), key.getPartitionId()), new OffsetAndMetadata(value, "MD", now));
    }

    // version 0 - zookeeper based offset management, 1 onwards is for kafka
    // topic based offset management
    OffsetCommitRequest commitRequest = new OffsetCommitRequest(getConsumerGroup(), offsets, getCorrelationId(),
        getClientId(), (short)1);
    setCorrelationId(getCorrelationId() + 1);

    channel.send(commitRequest.underlying());
    OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());

    if (commitResponse.hasError()) {
      int i = 0;

      while (i++ < DEFAULT_RETRY_COUNT && commitResponse.hasError()) {
        channel.send(commitRequest.underlying());
        commitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());
      }

      if (i == 3) {
        LOG.info("Failed to log the offsets");
      }
    }

  }

  @Override
  public Map<KafkaPartition, Long> loadInitialOffsets()
  {

    fetchOffsets();

    Map<KafkaPartition, Long> initialOffsetMap = new HashMap<KafkaPartition, Long>();

    for (Entry<TopicAndPartition, Long> entry : getOffsetsMap().entrySet()) {
      initialOffsetMap.put(new KafkaPartition(entry.getKey().topic(), entry.getKey().partition()), entry.getValue());
    }

    LOG.info("Offsets loaded");

    if (initialOffsetMap != null) {
      LOG.info("The initial offset map is:" + initialOffsetMap.toString());
    }

    return initialOffsetMap;
  }

}
