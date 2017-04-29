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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kafka.common.PartitionInfo;

import com.google.common.collect.Sets;

/**
 * An one-to-one partitioner implementation that always returns same amount of operator partitions as
 * Kafka partitions for the topics that operator subscribe
 *
 * @since 3.3.0
 */
@InterfaceStability.Evolving
public class OneToOnePartitioner extends AbstractKafkaPartitioner
{

  public OneToOnePartitioner(String[] clusters, String[] topics, AbstractKafkaInputOperator prototypeOperator)
  {
    super(clusters, topics, prototypeOperator);
  }

  @Override
  List<Set<PartitionMeta>> assign(Map<String, Map<String, List<PartitionInfo>>> metadata)
  {
    List<Set<PartitionMeta>> currentAssignment = new LinkedList<>();
    for (Map.Entry<String, Map<String, List<PartitionInfo>>> clusterMap : metadata.entrySet()) {
      for (Map.Entry<String, List<PartitionInfo>> topicPartition : clusterMap.getValue().entrySet()) {
        for (PartitionInfo pif : topicPartition.getValue()) {
          currentAssignment.add(Sets.newHashSet(new PartitionMeta(clusterMap.getKey(),
              topicPartition.getKey(), pif.partition())));
        }
      }
    }
    return currentAssignment;
  }

}
