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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Joiner;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;

/**
 * Abstract partitioner used to manage the partitions of kafka input operator.
 * It use a number of kafka consumers(one for each cluster) to get the latest partition metadata for topics that
 * the consumer subscribes and expose those to subclass which implements the assign method
 *
 * The partitioner is always stateless.
 */
public abstract class AbstractKafkaPartitioner implements Partitioner<AbstractKafkaInputOperator>, StatsListener
{

  private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaPartitioner.class);

  private static final String META_CONSUMER_GROUP_NAME = AbstractKafkaInputOperator.class.getName() + "META_GROUP";

  protected final String[] clusters;

  protected final String[] topics;

  protected final AbstractKafkaInputOperator prototypeOperator;

  private ArrayList<KafkaConsumer<byte[], byte[]>> metadataRefreshClients;


  private final List<Set<AbstractKafkaPartitioner.PartitionMeta>> currentPartitions = new LinkedList<>(); // prevent null

  public AbstractKafkaPartitioner(String[] clusters, String[] topics, AbstractKafkaInputOperator prototypeOperator)
  {
    this.clusters = clusters;
    this.topics = topics;
    this.prototypeOperator = prototypeOperator;
  }

  abstract List<Set<PartitionMeta>> assign(Map<String, Map<String,List<PartitionInfo>>> metadata);



  @Override
  public Collection<Partition<AbstractKafkaInputOperator>> definePartitions(Collection<Partition<AbstractKafkaInputOperator>> collection, PartitioningContext partitioningContext)
  {

    initMetadataClients();

    Map<String, Map<String, List<PartitionInfo>>> metadata = new HashMap<>();


    for (int i = 0; i < clusters.length; i++) {
      metadata.put(clusters[i], new HashMap<String, List<PartitionInfo>>());
      for (String topic : topics) {
        List<PartitionInfo> ptis = metadataRefreshClients.get(i).partitionsFor(topic);
        if (logger.isDebugEnabled()) {
          logger.debug("Partition metadata for topic {} : {}", topic, Joiner.on(';').join(ptis));
        }
        metadata.get(clusters[i]).put(topic, ptis);
      }
      metadataRefreshClients.get(i).close();
    }

    metadataRefreshClients = null;

    List<Set<AbstractKafkaPartitioner.PartitionMeta>> parts = assign(metadata);


    if (currentPartitions == parts || currentPartitions.equals(parts)) {
      logger.debug("No partition change found");
      return collection;
    } else {
      logger.info("Partition change detected: ");
      currentPartitions.clear();
      currentPartitions.addAll(parts);
      int i = 0;
      List<Partition<AbstractKafkaInputOperator>> result = new LinkedList<>();
      for (Iterator<Partition<AbstractKafkaInputOperator>> iter = collection.iterator(); iter.hasNext();) {
        Partition<AbstractKafkaInputOperator> nextPartition = iter.next();
        if (parts.remove(nextPartition.getPartitionedInstance().assignment())) {
          if (logger.isInfoEnabled()) {
            logger.info("[Existing] Partition {} with assignment {} ", i,
                Joiner.on(';').join(nextPartition.getPartitionedInstance().assignment()));
          }
          result.add(nextPartition);
          i++;
        }
      }

      for (Set<AbstractKafkaPartitioner.PartitionMeta> partitionAssignment : parts) {
        if (logger.isInfoEnabled()) {
          logger.info("[New] Partition {} with assignment {} ", i,
              Joiner.on(';').join(partitionAssignment));
        }
        result.add(createPartition(partitionAssignment));
        i++;
      }

      return result;
    }
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractKafkaInputOperator>> map)
  {

  }

  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    Response response = new Response();
    response.repartitionRequired = true;
    return response;
  }

  protected Partitioner.Partition<AbstractKafkaInputOperator> createPartition(Set<AbstractKafkaPartitioner.PartitionMeta> partitionAssignment)
  {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    kryo.writeObject(output, prototypeOperator);
    output.close();
    Input lInput = new Input(bos.toByteArray());
    Partitioner.Partition<AbstractKafkaInputOperator> p = (Partitioner.Partition<AbstractKafkaInputOperator>)
        new DefaultPartition<>(kryo.readObject(lInput, prototypeOperator.getClass()));
    p.getPartitionedInstance().assign(partitionAssignment);
    return p;
  }
  /**
   *
   */
  private void initMetadataClients()
  {
    if (metadataRefreshClients != null && metadataRefreshClients.size() == clusters.length) {
      // The metadata client is active
      return;
    }

    if (clusters == null || clusters.length == 0) {
      throw new IllegalStateException("clusters can not be null");
    }

    metadataRefreshClients = new ArrayList<>(clusters.length);
    int index = 0;
    for (String c : clusters) {
      Properties prop = new Properties();
      prop.put("group.id", META_CONSUMER_GROUP_NAME);
      prop.put("bootstrap.servers", c);
      prop.put("key.deserializer", ByteArrayDeserializer.class.getName());
      prop.put("value.deserializer", ByteArrayDeserializer.class.getName());
      prop.put("enable.auto.commit", "false");
      metadataRefreshClients.add(index++, new KafkaConsumer<byte[], byte[]>(prop));
    }
  }

  /**
   * The key object used in the assignment map for each operator
   */
  public static class PartitionMeta
  {

    public PartitionMeta()
    {
    }

    public PartitionMeta(String cluster, String topic, int partitionId)
    {
      this.cluster = cluster;
      this.topic = topic;
      this.partitionId = partitionId;
      this.topicPartition = new TopicPartition(topic, partitionId);
    }

    private String cluster;

    private transient TopicPartition topicPartition;

    private String topic;

    private int partitionId;

    public String getCluster()
    {
      return cluster;
    }

    public int getPartitionId()
    {
      return partitionId;
    }

    public String getTopic()
    {
      return topic;
    }

    public TopicPartition getTopicPartition()
    {
      if (topicPartition == null) {
        topicPartition = new TopicPartition(topic, partitionId);
      }
      return topicPartition;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionMeta that = (PartitionMeta)o;
      return Objects.equals(cluster, that.cluster) &&
        Objects.equals(getTopicPartition(), that.getTopicPartition());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(cluster, getTopicPartition());
    }

    @Override
    public String toString()
    {
      return "PartitionMeta{" +
        "cluster='" + cluster + '\'' +
        ", topicPartition=" + topicPartition +
        '}';
    }
  }
}
