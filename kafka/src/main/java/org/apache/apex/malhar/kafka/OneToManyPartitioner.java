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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kafka.common.PartitionInfo;

/**
 * A one-to-many partitioner implementation that creates fix number of operator partitions and assign one or more
 * Kafka partitions to each. It use round robin to assign partitions
 *
 * @since 3.3.0
 */
@InterfaceStability.Evolving
public class OneToManyPartitioner extends AbstractKafkaPartitioner
{

  public OneToManyPartitioner(String[] clusters, String[] topics, AbstractKafkaInputOperator protoTypeOperator)
  {
    super(clusters, topics, protoTypeOperator);
  }

  @Override
  List<Set<PartitionMeta>> assign(Map<String, Map<String, List<PartitionInfo>>> metadata)
  {
    if (prototypeOperator.getInitialPartitionCount() <= 0) {
      throw new IllegalArgumentException("Num of partitions should be greater or equal to 1");
    }

    int partitionCount = prototypeOperator.getInitialPartitionCount();
    ArrayList<Set<PartitionMeta>> eachPartitionAssignment =
        new ArrayList<>(prototypeOperator.getInitialPartitionCount());
    int i = 0;
    for (Map.Entry<String, Map<String, List<PartitionInfo>>> clusterMap : metadata.entrySet()) {
      for (Map.Entry<String, List<PartitionInfo>> topicPartition : clusterMap.getValue().entrySet()) {
        for (PartitionInfo pif : topicPartition.getValue()) {
          int index = i++ % partitionCount;
          if (index >= eachPartitionAssignment.size()) {
            eachPartitionAssignment.add(new HashSet<PartitionMeta>());
          }
          eachPartitionAssignment.get(index).add(new PartitionMeta(clusterMap.getKey(),
              topicPartition.getKey(), pif.partition()));
        }
      }
    }

    return eachPartitionAssignment;
  }

}
