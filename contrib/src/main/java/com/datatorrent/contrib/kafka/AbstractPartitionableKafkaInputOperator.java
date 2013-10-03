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

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;

import com.datatorrent.api.PartitionableOperator;

/**
 * This kafka input operator which is automatically partitioned as the upstream
 * kafka partition This operator only use simple kafka consumer because there is
 * no partition metadata support for high level kafka consumer To use high level
 * kafka consumer you have to manually create multiple
 * AbstractKafkaInputOperator in DAG and set correct groupid which read specific
 * kafka partition
 */
public abstract class AbstractPartitionableKafkaInputOperator extends AbstractKafkaInputOperator<SimpleKafkaConsumer> implements PartitionableOperator
{

  private static final String CLIENT_NAME_SUFFIX = "_Partition";

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity)
  {
    getConsumer().create();
    List<PartitionMetadata> kafkaPartitionList = getConsumer().getPartitionMDs();
    List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(kafkaPartitionList.size());
    Iterator<Partition<AbstractPartitionableKafkaInputOperator>> iterator = (Iterator<Partition<AbstractPartitionableKafkaInputOperator>>) partitions.iterator();
    Partition<AbstractPartitionableKafkaInputOperator> templatePartition = iterator.next();
    for (int i = 0; i < kafkaPartitionList.size(); i++) {
      Partition<AbstractPartitionableKafkaInputOperator> p = templatePartition.getInstance(cloneOperator());
      SimpleKafkaConsumer sfc = this.getConsumer();
      PartitionMetadata pm = kafkaPartitionList.get(i);
      Broker b = pm.leader();
      SimpleKafkaConsumer copy_Of_SFC = new SimpleKafkaConsumer(sfc.topic, b.host(), b.port(), sfc.getTimeout(), sfc.getBufferSize(), sfc.getClientId() + CLIENT_NAME_SUFFIX + pm.partitionId(), pm.partitionId());
      p.getOperator().setConsumer(copy_Of_SFC);
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

}
