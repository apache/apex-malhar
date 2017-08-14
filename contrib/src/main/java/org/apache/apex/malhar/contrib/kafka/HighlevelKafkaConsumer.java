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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

/**
 * High level kafka consumer adapter used for kafka input operator Properties:<br>
 * <b>consumerConfig</b>: Used for create the high-level kafka consumer<br>
 * <b>numStream</b>: num of threads to consume the topic in parallel <br>
 * <li>(-1): create #partition thread and consume the topic in parallel threads</li> <br>
 * <br>
 *
 * Load balance: <br>
 * Build-in kafka load balancing strategy, Consumers with different consumer.id and same group.id will distribute the
 * reads from different partition<br>
 * There are at most #partition per topic could consuming in parallel For more information see {@link http
 * ://kafka.apache.org/documentation.html#distributionimpl} <br>
 * <br>
 * <br>
 * Kafka broker failover: <br>
 * Build-in failover strategy, the consumer will pickup the next available synchronized broker to consume data <br>
 * For more information see {@link http ://kafka.apache.org/documentation.html#distributionimpl} <br>
 *
 * @since 0.9.0
 */
public class HighlevelKafkaConsumer extends KafkaConsumer
{
  private static final Logger logger = LoggerFactory.getLogger(HighlevelKafkaConsumer.class);

  private Properties consumerConfig = null;

  /**
   * Consumer client for topic on each cluster
   */
  private transient Map<String, ConsumerConnector> standardConsumer = null;

  private transient ExecutorService consumerThreadExecutor = null;

  /**
   * number of stream for topic on each cluster null/empty: create same # streams to # partitions of the topic on each
   * cluster
   */
  private Map<String, Integer> numStream;

  public HighlevelKafkaConsumer()
  {
    numStream = new HashMap<String, Integer>();
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
    if (standardConsumer == null) {
      standardConsumer = new HashMap<String, ConsumerConnector>();
    }

    // This is important to let kafka know how to distribute the reads among
    // different consumers in same consumer group
    // Don't reuse any id for recovery to avoid rebalancing error because
    // there is some delay for zookeeper to
    // find out the old consumer is dead and delete the entry even new
    // consumer is back online
    consumerConfig.put("consumer.id", "consumer" + System.currentTimeMillis());
    if (initialOffset.equalsIgnoreCase("earliest")) {
      consumerConfig.put("auto.offset.reset", "smallest");
    } else {
      consumerConfig.put("auto.offset.reset", "largest");
    }

  }

  @Override
  public void start()
  {
    super.start();
    //Share other properties among all connectors but set zookeepers respectively cause different cluster would use different zookeepers
    for (String cluster : zookeeperMap.keySet()) {
      // create high level consumer for every cluster
      Properties config = new Properties();
      config.putAll(consumerConfig);
      config.setProperty("zookeeper.connect", zookeeperMap.get(cluster).iterator().next());
      // create consumer connector will start a daemon thread to monitor the metadata change
      // we want to start this thread until the operator is activated
      standardConsumer.put(cluster, kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(config)));
    }

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

    if (numStream == null || numStream.size() == 0) {
      if (numStream == null) {
        numStream = new HashMap<String, Integer>();
      }
      // get metadata from kafka and initialize streams accordingly
      for (Entry<String, List<PartitionMetadata>> e : KafkaMetadataUtil.getPartitionsForTopic(brokers, topic).entrySet()) {
        numStream.put(e.getKey(), e.getValue().size());
      }
    }

    int totalNumStream = 0;
    for (int delta : numStream.values()) {
      totalNumStream += delta;
    }
    // start $totalNumStream anonymous threads to consume the data from all clusters
    if (totalNumStream <= 0) {
      logger.warn("No more job needed to consume data ");
      return;
    }
    consumerThreadExecutor = Executors.newFixedThreadPool(totalNumStream);

    for (final Entry<String, Integer> e : numStream.entrySet()) {

      int realNumStream = e.getValue();
      topicCountMap.put(topic, new Integer(realNumStream));
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = standardConsumer.get(e.getKey()).createMessageStreams(topicCountMap);

      for (final KafkaStream<byte[], byte[]> stream : consumerMap.get(topic)) {
        consumerThreadExecutor.submit(new Runnable()
        {
          KafkaPartition kp = new KafkaPartition(e.getKey(), topic, -1);

          public void run()
          {
            ConsumerIterator<byte[], byte[]> itr = stream.iterator();
            logger.debug("Thread {} starts consuming message...", Thread.currentThread().getName());
            while (itr.hasNext() && isAlive) {
              MessageAndMetadata<byte[], byte[]> mam = itr.next();
              try {
                kp.setPartitionId(mam.partition());
                putMessage(kp, new Message(mam.message()), mam.offset());
              } catch (InterruptedException e) {
                logger.error("Message Enqueue has been interrupted", e);
              }
            }
            logger.debug("Thread {} stops consuming message...", Thread.currentThread().getName());
          }
        });
      }

    }

  }

  @Override
  public void close()
  {
    if (standardConsumer != null && standardConsumer.values() != null) {
      for (ConsumerConnector consumerConnector : standardConsumer.values()) {
        consumerConnector.shutdown();
      }
    }
    if (consumerThreadExecutor != null) {
      consumerThreadExecutor.shutdown();
    }
  }

  public void setConsumerConfig(Properties consumerConfig)
  {
    this.consumerConfig = consumerConfig;
  }

  @Override
  protected void resetPartitionsAndOffset(Set<KafkaPartition> partitionIds, Map<KafkaPartition, Long> startOffset)
  {
    this.numStream = new HashMap<String, Integer>();
    for (KafkaPartition kafkaPartition : partitionIds) {
      if (this.numStream.get(kafkaPartition.getClusterId()) == null) {
        this.numStream.put(kafkaPartition.getClusterId(), 0);
      }
      this.numStream.put(kafkaPartition.getClusterId(), this.numStream.get(kafkaPartition.getClusterId()) + 1);
    }
  }

  @Override
  protected void commitOffset()
  {
    // commit the offsets at checkpoint so that high-level consumer don't
    // have to receive too many duplicate messages
    if (standardConsumer != null && standardConsumer.values() != null) {
      for (ConsumerConnector consumerConnector : standardConsumer.values()) {
        consumerConnector.commitOffsets();
      }
    }
  }

  @Override
  protected Map<KafkaPartition, Long> getCurrentOffsets()
  {
    // offset is not useful for high-level kafka consumer
    throw new UnsupportedOperationException("Offset request is currently not supported for high-level consumer");
  }

}
