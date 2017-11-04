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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

/**
 * Interface for Kafka Consumer. It wraps around the KafkaConsumer.
 *
 * @since 3.8.0
 */
public interface AbstractKafkaConsumer
{
  /**
   * Checks whether the consumer contains the specified partition or not
   * @param topicPartition topic partition
   * @return true if consumer contains the given partition, otherwise false
   */
  boolean isConsumerContainsPartition(TopicPartition topicPartition);

  /**
   * Seek to the specified offset for the given partition
   * @param topicPartition topic partition
   * @param offset given offset
   */
  void seekToOffset(TopicPartition topicPartition, long offset);

  /**
   * Fetch data for the topics or partitions specified using assign API.
   * @param timeOut time in milliseconds, spent waiting in poll if data is not available in buffer.
   * @return records
   */
  ConsumerRecords<byte[], byte[]> pollRecords(long timeOut);

  /**
   * Commit the specified offsets for the specified list of topics and partitions to Kafka.
   * @param offsets given offsets
   * @param callback Callback to invoke when the commit completes
   */
  void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

  /**
   * Assign the specified list of partitions to the consumer
   * @param partitions list of partitions
   */
  void assignPartitions(List<TopicPartition> partitions);

  /**
   * Seek to the first offset for the specified list of partitions
   * @param partitions list of partitions
   */
  void seekToBeginning(TopicPartition... partitions);

  /**
   * Seek to the last offser for the specified list of partitions
   * @param partitions list of partitions
   */
  void seekToEnd(TopicPartition... partitions);

  /**
   * Wrapper for Wakeup the consumer
   */
  void wakeup();

  /**
   * Return the metrics kept by the consumer
   * @return metrics
   */
  Map<MetricName, ? extends Metric> metrics();

  /**
   * Wrapper for close the consumer
   */
  void close();

  /**
   * Resume all the partitions
   */
  void resumeAllPartitions();

  /**
   * Return the list of partitions assigned to this consumer
   * @return list of partitions
   */
  Collection<TopicPartition> getPartitions();

  /**
   * Resume the specified partition
   * @param tp partition
   */
  void resumePartition(TopicPartition tp);

  /**
   * Pause the specified partition
   * @param tp partition
   */
  void pausePartition(TopicPartition tp);

  /**
   * Return the offset of the next record that will be fetched
   * @param tp partition
   */
  long positionPartition(TopicPartition tp);
}
