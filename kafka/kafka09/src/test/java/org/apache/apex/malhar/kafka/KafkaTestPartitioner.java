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

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import kafka.utils.VerifiableProperties;

/**
 * A simple partitioner class for test purpose
 * Key is a int string
 * Messages are distributed to all partitions
 * One for even number, the other for odd
 */
public class KafkaTestPartitioner implements Partitioner
{
  public KafkaTestPartitioner(VerifiableProperties props)
  {

  }

  public KafkaTestPartitioner()
  {

  }

  @Override
  public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster)
  {
    int num_partitions = cluster.partitionsForTopic(topic).size();
    return Integer.parseInt((String)key) % num_partitions;
  }

  @Override
  public void close()
  {

  }

  @Override
  public void configure(Map<String, ?> map)
  {

  }
}
