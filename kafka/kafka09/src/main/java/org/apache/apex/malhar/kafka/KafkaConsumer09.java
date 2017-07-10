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
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.Iterables;

/**
 * Wrapper for 0.9.x version of Kafka consumer
 */
@InterfaceStability.Evolving
public class KafkaConsumer09<K, V> implements AbstractKafkaConsumer
{
  private KafkaConsumer<K, V> consumer;

  public KafkaConsumer09(Properties properties)
  {
    consumer = new KafkaConsumer<>(properties);
  }

  /**
   * Checks whether the consumer contains the specified partition or not
   * @param topicPartition topic partition
   * @return true if consumer contains the given partition, otherwise false
   */
  @Override
  public boolean isConsumerContainsPartition(TopicPartition topicPartition)
  {
    return consumer.assignment().contains(topicPartition);
  }

  /**
   * Seek to the specified offset for the given partition
   * @param topicPartition topic partition
   * @param offset given offset
   */
  @Override
  public void seekToOffset(TopicPartition topicPartition, long offset)
  {
    consumer.seek(topicPartition, offset);
  }

  /**
   * Fetch data for the topics or partitions specified using assign API.
   * @param timeOut time in milliseconds, spent waiting in poll if data is not available in buffer.
   * @return records
   */
  @Override
  public ConsumerRecords pollRecords(long timeOut)
  {
    return consumer.poll(timeOut);
  }

  /**
   * Commit the specified offsets for the specified list of topics and partitions to Kafka.
   * @param offsets given offsets
   * @param callback Callback to invoke when the commit completes
   */
  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback)
  {
    consumer.commitAsync(offsets, callback);
  }

  /**
   * Assign the specified list of partitions to the consumer
   * @param partitions list of partitions
   */
  @Override
  public void assignPartitions(List<TopicPartition> partitions)
  {
    consumer.assign(partitions);
  }

  /**
   * Seek to the first offset for the specified list of partitions
   * @param partitions list of partitions
   */
  @Override
  public void seekToBeginning(TopicPartition... partitions)
  {
    consumer.seekToBeginning(partitions);
  }

  /**
   * Seek to the last offser for the specified list of partitions
   * @param partitions list of partitions
   */
  @Override
  public void seekToEnd(TopicPartition... partitions)
  {
    consumer.seekToEnd(partitions);
  }

  /**
   * Wrapper for Wakeup the consumer
   */
  @Override
  public void wakeup()
  {
    consumer.wakeup();
  }

  /**
   * Return the metrics kept by the consumer
   * @return metrics
   */
  @Override
  public Map<MetricName, ? extends Metric> metrics()
  {
    return consumer.metrics();
  }

  /**
   * Wrapper for close the consumer
   */
  @Override
  public void close()
  {
    consumer.close();
  }

  /**
   * Resume all the partitions
   */
  @Override
  public void resumeAllPartitions()
  {
    consumer.resume(Iterables.toArray(this.getPartitions(),TopicPartition.class));
  }

  /**
   * Return the list of partitions assigned to this consumer
   * @return list of partitions
   */
  @Override
  public Collection<TopicPartition> getPartitions()
  {
    return consumer.assignment();
  }

  /**
   * Resume the specified partition
   * @param tp partition
   */
  @Override
  public void resumePartition(TopicPartition tp)
  {
    consumer.resume(tp);
  }

  /**
   * Pause the specified partition
   * @param tp partition
   */
  @Override
  public void pausePartition(TopicPartition tp)
  {
    consumer.pause(tp);
  }

  /**
   * Return the offset of the next record that will be fetched
   * @param tp partition
   */
  @Override
  public long positionPartition(TopicPartition tp)
  {
    return consumer.position(tp);
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic)
  {
    return consumer.partitionsFor(topic);
  }
}
