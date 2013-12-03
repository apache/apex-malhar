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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.javaapi.PartitionMetadata;
import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitionable;

/**
 *
 * This kafka input operator will be automatically partitioned per upstream kafka partition.<br> <br>
 * This is not real dynamic partition, The partition number is decided by number of partition set for the topic in kafka.<br> <br>
 *
 * <b>Algorithm:</b> <br>
 * <p>1.Pull the metadata(how many partitions) of the topic from brokerList of {@link KafkaConsumer}</p>
 * <p>2.Create new partition according to how many partitions are there for the topic</p>
 * <p>3.cloneConsumer method is used to initialize the new {@link KafkaConsumer} instance for the new partition operator</p>
 * <p>4.cloneOperator method is used to initialize the new {@link AbstractPartitionableKafkaInputOperator} instance for the new partition operator</p>
 * <br>
 * <br>
 * <b>Load balance:</b> refer to {@link SimpleKafkaConsumer} and {@link HighlevelKafkaConsumer} <br>
 * <b>Kafka partition failover:</b> refer to {@link SimpleKafkaConsumer} and {@link HighlevelKafkaConsumer}
 *
 * @since 0.9.0
 */
public abstract class AbstractPartitionableKafkaInputOperator extends AbstractKafkaInputOperator<KafkaConsumer> implements Partitionable<AbstractPartitionableKafkaInputOperator>, CheckpointListener
{

  private static final Logger logger = LoggerFactory.getLogger(AbstractPartitionableKafkaInputOperator.class);

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition<AbstractPartitionableKafkaInputOperator>> definePartitions(Collection<Partition<AbstractPartitionableKafkaInputOperator>> partitions, int incrementalCapacity)
  {
    // get partition metadata for topics.
    // Whatever operator is using high-level or simple kafka consumer, the operator always create a temporary simple kafka consumer to get the metadata of the topic
    // The initial value of brokerList of the KafkaConsumer is used to retrieve the topic metadata
    List<PartitionMetadata> kafkaPartitionList = KafkaMetadataUtil.getPartitionsForTopic(getConsumer().getBrokerSet(), getConsumer().getTopic());
    // There are *AT MOST* #partition exclusive consumers for each topic of kafka message
    // If you want to create more partition for operator, you can create more partitions for kafka data feed
    List<Partition<AbstractPartitionableKafkaInputOperator>> newPartitions = new ArrayList<Partition<AbstractPartitionableKafkaInputOperator>>(kafkaPartitionList.size());

    // Get template partition
    Iterator<Partition<AbstractPartitionableKafkaInputOperator>> iterator = partitions.iterator();
    Partition<AbstractPartitionableKafkaInputOperator> templatePartition = iterator.next();

    // Create new partition from template partition but use pass-in partition ID
    for (int i = 0; i < kafkaPartitionList.size(); i++) {
      logger.debug("Create partition " + kafkaPartitionList.get(i).partitionId());
      Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(cloneOperator());
      PartitionMetadata pm = kafkaPartitionList.get(i);
      KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(pm.partitionId());
      p.getPartitionedInstance().setConsumer(newConsumerForPartition);
      newPartitions.add(p);
    }
    return newPartitions;
  }

  /**
   * Implement this method to initialize new operator instance for new partition.
   * Please carefully include all the properties you want to clone to new instance
   * @return
   */
  protected abstract AbstractPartitionableKafkaInputOperator cloneOperator();

  @Override
  public void checkpointed(long windowId)
  {
    // commit the kafka consummer offset
    getConsumer().commitOffset();
  }

  @Override
  public void committed(long windowId)
  {
  }

}
