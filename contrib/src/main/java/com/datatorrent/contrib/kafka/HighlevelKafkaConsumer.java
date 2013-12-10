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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

/**
 * High level kafka consumer adapter used for kafka input operator
 * Properties:<br>
 * <b>consumerConfig</b>: Used for create the high-level kafka consumer<br>
 * <b>numStream</b>: num of threads to consume the topic in parallel <br>
 * <li> (-1): create #partition thread and consume the topic in parallel threads</li>
 * <br>
 * <br>
 *
 * Load balance: <br>
 * Build-in kafka load balancing strategy, Consumers with different consumer.id and same group.id will distribute the reads from different partition<br>
 * There are at most #partition per topic could consuming in parallel
 * For more information see {@link http://kafka.apache.org/documentation.html#distributionimpl} <br>
 * <br><br>
 * Kafka broker failover: <br>
 * Build-in failover strategy, the consumer will pickup the next available synchronized broker to consume data <br>
 * For more information see {@link http://kafka.apache.org/documentation.html#distributionimpl} <br>
 *
 * @since 0.9.0
 */
public class HighlevelKafkaConsumer extends KafkaConsumer
{
  private static final Logger logger = LoggerFactory.getLogger(HighlevelKafkaConsumer.class);
  
  private Properties consumerConfig = null;

  private transient ConsumerConnector standardConsumer = null;
  
  private transient ExecutorService consumerThreadExecutor = null;
  
  /**
   * -1   Dynamically create number of stream according to the partitions
   * < #{kafka partition} each stream could receive any message from any partition, order is not guaranteed among the partitions
   * > #{kafka partition} each stream consume message from one partition, some stream might not get any data
   */
  @Min(value = -1)
  private int numStream = 1;
  
  public HighlevelKafkaConsumer()
  {
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
    // This is important to let kafka know how to distribute the reads among different consumers in same consumer group
    // Don't reuse any id for recovery to avoid rebalancing error because there is some delay for zookeeper to 
    // find out the old consumer is dead and delete the entry even new consumer is back online
    consumerConfig.put("consumer.id", "consumer" + System.currentTimeMillis());
    if(initialOffset.equalsIgnoreCase("earliest")){
      consumerConfig.put("auto.offset.reset", "smallest");
    } else {
      consumerConfig.put("auto.offset.reset", "largest");
    }
    standardConsumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerConfig));
  }

  @Override
  public void start()
  {
    super.start();
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    int realNumStream = numStream;
    if (numStream == -1) {
      realNumStream = KafkaMetadataUtil.getPartitionsForTopic(getBrokerSet(), getTopic()).size();
    }
    topicCountMap.put(topic, new Integer(realNumStream));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = standardConsumer.createMessageStreams(topicCountMap);

    // start $numStream anonymous threads to consume the data
    consumerThreadExecutor = Executors.newFixedThreadPool(realNumStream);
    for (final KafkaStream<byte[], byte[]> stream : consumerMap.get(topic)) {
      consumerThreadExecutor.submit(new Runnable() {
        public void run()
        {
          ConsumerIterator<byte[], byte[]> itr = stream.iterator();
          logger.debug("Thread " + Thread.currentThread().getName() + " start consuming message...");
          while (itr.hasNext() && isAlive) {
            MessageAndMetadata<byte[], byte[]> mam = itr.next();
            try {
              putMessage(mam.partition(), new Message(mam.message()));
            } catch (InterruptedException e) {
              logger.error("Message Enqueue has been interrupted", e);
            }
          }
          logger.debug("Thread " + Thread.currentThread().getName() + " stop consuming message...");
        }
      });
    }
  }

  @Override
  protected void _stop()
  {
    if(standardConsumer!=null)
      standardConsumer.shutdown();
    if(consumerThreadExecutor!=null){
      consumerThreadExecutor.shutdown();
    }
  }

  
  public void setConsumerConfig(Properties consumerConfig)
  {
    this.consumerConfig = consumerConfig;
  }

  @Override
  protected KafkaConsumer cloneConsumer(Set<Integer> partitionIds)
  {
    return cloneConsumer(partitionIds, null);
  }
  
  @Override
  protected KafkaConsumer cloneConsumer(Set<Integer> partitionIds, Map<Integer, Long> startOffset)
  {
    Properties newProp = new Properties();
    // Copy most properties from the template consumer. For example the "group.id" should be set to same value 
    newProp.putAll(consumerConfig);
    HighlevelKafkaConsumer newConsumer = new HighlevelKafkaConsumer(newProp);
    newConsumer.setBrokerSet(this.brokerSet);
    newConsumer.setTopic(this.topic);
    newConsumer.numStream = partitionIds.size();
    newConsumer.initialOffset = initialOffset;
    return newConsumer;
  }

  @Override
  protected void commitOffset()
  {
    // commit the offsets at checkpoint so that high-level consumer don't have to receive too many duplicate messages
    standardConsumer.commitOffsets();
  }

  @Override
  protected Map<Integer, Long> getCurrentOffsets()
  {
    // offset is not useful for high-level kafka consumer
    // TODO 
    throw new UnsupportedOperationException("Offset request is currently not supported for high-level consumer");
  }
  
}
