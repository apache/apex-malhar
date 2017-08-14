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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import com.google.common.collect.SetMultimap;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import scala.collection.JavaConversions;

/**
 * A util class used to retrieve all the metadatas for partitions/topics
 * Every method in the class creates a temporary simple kafka consumer and
 * release the resource immediately after retrieving the metadata
 *
 * @since 0.9.0
 */
public class KafkaMetadataUtil
{

  public static final String PRODUCER_PROP_PARTITIONER = "partitioner.class";

  public static final String PRODUCER_PROP_BROKERLIST = "metadata.broker.list";

  private static Logger logger = LoggerFactory.getLogger(KafkaMetadataUtil.class);

  // A temporary client used to retrieve the metadata of topic/partition etc
  private static final String mdClientId = "Kafka_Metadata_Lookup_Client";

  private static final int timeout = 10000;

  //buffer size for MD lookup client is 128k should be enough for most cases
  private static final int bufferSize = 128 * 1024;

  /**
   * @param brokerList brokers in same cluster
   * @param topic
   * @return Get the partition metadata list for the specific topic via the brokerList <br>
   * null if topic is not found
   */
  public static List<PartitionMetadata> getPartitionsForTopic(Set<String> brokerList, String topic)
  {
    TopicMetadata tmd = getTopicMetadata(brokerList, topic);
    if (tmd == null) {
      return null;
    }
    return tmd.partitionsMetadata();
  }

  /**
   * @param brokers in multiple clusters, keyed by cluster id
   * @param topic
   * @return Get the partition metadata list for the specific topic via the brokers
   * null if topic is not found
   */
  public static Map<String, List<PartitionMetadata>> getPartitionsForTopic(SetMultimap<String, String> brokers, final String topic)
  {
    return Maps.transformEntries(brokers.asMap(), new EntryTransformer<String, Collection<String>, List<PartitionMetadata>>()
    {
      @Override
      public List<PartitionMetadata> transformEntry(String key, Collection<String> bs)
      {
        return getPartitionsForTopic(new HashSet<String>(bs), topic);
      }
    });
  }

  /**
   * There is always only one string in zkHost
   * @param zkHost
   * @return
   */
  public static Set<String> getBrokers(Set<String> zkHost)
  {
    ZkClient zkclient = new ZkClient(zkHost.iterator().next(), 30000, 30000, ZKStringSerializer$.MODULE$);
    Set<String> brokerHosts = new HashSet<String>();
    for (Broker b : JavaConversions.asJavaIterable(ZkUtils.getAllBrokersInCluster(zkclient))) {
      brokerHosts.add(b.connectionString());
    }
    zkclient.close();
    return brokerHosts;
  }

  /**
   * @param brokerList
   * @param topic
   * @param partition
   * @return Get the partition metadata for specific topic and partition via the brokerList<br>
   * null if topic is not found
   */
  public static PartitionMetadata getPartitionForTopic(Set<String> brokerList, String topic, int partition)
  {
    List<PartitionMetadata> pmds = getPartitionsForTopic(brokerList, topic);
    if (pmds == null) {
      return null;
    }
    for (PartitionMetadata pmd : pmds) {
      if (pmd.partitionId() != partition) {
        continue;
      }
      return pmd;
    }
    return null;
  }

  /**
   * @param brokerSet
   * @param topic
   * @return TopicMetadata for this specific topic via the brokerList<br>
   * null if topic is not found
   */
  public static TopicMetadata getTopicMetadata(Set<String> brokerSet, String topic)
  {
    SimpleConsumer mdConsumer = null;
    if (brokerSet == null || brokerSet == null || brokerSet.size() == 0) {
      return null;
    }
    try {
      for (Iterator<String> iterator = brokerSet.iterator(); iterator.hasNext();) {
        String broker = iterator.next();
        logger.debug("Try to get Metadata for topic {} broker {}", topic, broker);
        try {
          mdConsumer = new SimpleConsumer(broker.split(":")[0], Integer.parseInt(broker.split(":")[1]), timeout, bufferSize, mdClientId);

          List<String> topics = new ArrayList<String>(1);
          topics.add(topic);
          kafka.javaapi.TopicMetadataRequest req = new kafka.javaapi.TopicMetadataRequest(topics);
          TopicMetadataResponse resp = mdConsumer.send(req);
          List<TopicMetadata> metaData = resp.topicsMetadata();
          for (TopicMetadata item : metaData) {
            // There is at most 1 topic for this method
            return item;
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Wrong format for broker url, should be \"broker1:port1\"");
        } catch (Exception e) {
          logger.warn("Broker {} is unavailable or in bad state!", broker);
          // skip and try next broker
        }
      }

      return null;
    } finally {
      if (mdConsumer != null) {
        mdConsumer.close();
      }
    }
  }

  /**
   * @param consumer
   * @param topic
   * @param partition
   * @param whichTime
   * @param clientName
   * @return 0 if consumer is null at this time
   */
  public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName)
  {
    if (consumer == null) {
      return 0;
    }
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
    OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      logger.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
      return 0;
    }
    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }

}
