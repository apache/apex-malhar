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

import java.io.Closeable;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Pattern.Flag;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.datatorrent.api.Context;
import kafka.message.Message;

/**
 * Base Kafka Consumer class used by kafka input operator
 *
 * @since 0.9.0
 */
public abstract class KafkaConsumer implements Closeable
{
  protected static final String HIGHLEVEL_CONSUMER_ID_SUFFIX = "_stream_";

  protected static final String SIMPLE_CONSUMER_ID_SUFFIX = "_partition_";
  private String zookeeper;

  public KafkaConsumer()
  {
  }

  public KafkaConsumer(String topic)
  {
    this();
    this.topic = topic;
  }

  public KafkaConsumer(String zks, String topic)
  {
    this.topic = topic;
    setZookeeper(zks);
  }

  private int cacheSize = 1024;

  protected transient boolean isAlive = false;

  private transient ArrayBlockingQueue<KafkaMessage> holdingBuffer;

  /**
   * The topic that this consumer consumes
   */
  @NotNull
  protected String topic = "default_topic";

  /**
   * A zookeeper map keyed by cluster id.
   * It's mandatory field <br>
   * Each cluster should have only one connection string contain all nodes in the cluster <br>
   * zookeeper chroot path is also supported <br>
   *
   * Single cluster zookeeper example: <br>
   * &nbsp;&nbsp;  node1:2181,node2:2181,node3:2181/your/kafka/data <br>
   * Multi-cluster zookeeper example: <br>
   * &nbsp;&nbsp;  cluster1::node1:2181,node2:2181,node3:2181/cluster1;cluster2::node1:2181/cluster2
   *
   */
  @NotNull
  @Bind(JavaSerializer.class)
  SetMultimap<String, String> zookeeperMap;

  protected transient SetMultimap<String, String> brokers;


  /**
   * The initialOffset could be either earliest or latest
   * Earliest means the beginning the queue
   * Latest means the current sync point to consume the queue
   * This setting is case_insensitive
   * By default it always consume from the beginning of the queue
   */
  @Pattern(flags = {Flag.CASE_INSENSITIVE}, regexp = "earliest|latest")
  protected String initialOffset = "latest";


  protected transient SnapShot statsSnapShot = new SnapShot();

  protected transient KafkaMeterStats stats = new KafkaMeterStats();

  /**
   * This method is called in setup method of the operator
   */
  public void create()
  {
    initBrokers();
    holdingBuffer = new ArrayBlockingQueue<KafkaMessage>(cacheSize);
  }

  public void initBrokers()
  {
    if (brokers != null) {
      return;
    }
    if (zookeeperMap != null) {
      brokers = HashMultimap.create();
      for (String clusterId: zookeeperMap.keySet()) {
        try {
          brokers.putAll(clusterId, KafkaMetadataUtil.getBrokers(zookeeperMap.get(clusterId)));
        } catch (Exception e) {
          // let the user know where we tried to connect to
          throw new RuntimeException("Error resolving brokers for cluster " + clusterId + " " + zookeeperMap.get(clusterId), e);
        }
      }
    }

  }

  /**
   * This method is called in the activate method of the operator
   */
  public void start()
  {
    isAlive = true;
    statsSnapShot.start();
  }

  /**
   * The method is called in the deactivate method of the operator
   */
  public void stop()
  {
    isAlive = false;
    statsSnapShot.stop();
    holdingBuffer.clear();
    IOUtils.closeQuietly(this);
  }

  /**
   * This method is called in teardown method of the operator
   */
  public void teardown()
  {
    holdingBuffer.clear();
  }

  public boolean isAlive()
  {
    return isAlive;
  }

  public void setAlive(boolean isAlive)
  {
    this.isAlive = isAlive;
  }

  /**
   * Set the Topic.
   */
  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public String getTopic()
  {
    return topic;
  }

  public KafkaMessage pollMessage()
  {
    return holdingBuffer.poll();
  }

  public int messageSize()
  {
    return holdingBuffer.size();
  }

  public void setInitialOffset(String initialOffset)
  {
    this.initialOffset = initialOffset;
  }

  public String getInitialOffset()
  {
    return initialOffset;
  }

  public int getCacheSize()
  {
    return cacheSize;
  }

  public void setCacheSize(int cacheSize)
  {
    this.cacheSize = cacheSize;
  }


  protected final void putMessage(KafkaPartition partition, Message msg, long offset) throws InterruptedException
  {
    // block from receiving more message
    holdingBuffer.put(new KafkaMessage(partition, msg, offset));
    statsSnapShot.mark(partition, msg.payloadSize());
  }

  protected abstract void commitOffset();

  protected abstract Map<KafkaPartition, Long> getCurrentOffsets();

  public KafkaMeterStats getConsumerStats()
  {
    statsSnapShot.setupStats(stats);
    return stats;
  }

  protected abstract void resetPartitionsAndOffset(Set<KafkaPartition> partitionIds, Map<KafkaPartition, Long> startOffset);

  /**
   * Set the ZooKeeper quorum of the Kafka cluster(s) you want to consume data from.
   * The operator will discover the brokers that it needs to consume messages from.
   */
  public void setZookeeper(String zookeeper)
  {
    this.zookeeper = zookeeper;
    this.zookeeperMap = parseZookeeperStr(zookeeper);
  }

  public String getZookeeper()
  {
    return zookeeper;
  }

  /**
   * Counter class which gives the statistic value from the consumer
   */
  public static class KafkaMeterStats implements Serializable
  {

    private static final long serialVersionUID = -2867402654990209006L;

    /**
     * A compact partition counter. The data collected for each partition is 4bytes brokerId + 1byte connected + 8bytes msg/s + 8bytes bytes/s + 8bytes offset
     */
    public ConcurrentHashMap<KafkaPartition, PartitionStats> partitionStats = new ConcurrentHashMap<KafkaPartition, PartitionStats>();


    /**
     * A compact overall counter. The data collected is 4bytes connection number + 8bytes aggregate msg/s + 8bytes aggregate bytes/s
     */
    public long totalMsgPerSec;

    public long totalBytesPerSec;


    public KafkaMeterStats()
    {

    }

    public void set_1minMovingAvgPerPartition(KafkaPartition kp, long[] _1minAvgPar)
    {
      PartitionStats ps = putPartitionStatsIfNotPresent(kp);
      ps.msgsPerSec = _1minAvgPar[0];
      ps.bytesPerSec = _1minAvgPar[1];
    }

    public void set_1minMovingAvg(long[] _1minAvg)
    {
      totalMsgPerSec = _1minAvg[0];
      totalBytesPerSec = _1minAvg[1];
    }

    public void updateOffsets(Map<KafkaPartition, Long> offsets)
    {
      for (Entry<KafkaPartition, Long> os : offsets.entrySet()) {
        PartitionStats ps = putPartitionStatsIfNotPresent(os.getKey());
        ps.offset = os.getValue();
      }
    }

    public int getConnections()
    {
      int r = 0;
      for (PartitionStats ps : partitionStats.values()) {
        if (!StringUtils.isEmpty(ps.brokerHost)) {
          r++;
        }
      }
      return r;
    }

    public void updatePartitionStats(KafkaPartition kp,int brokerId, String host)
    {
      PartitionStats ps = putPartitionStatsIfNotPresent(kp);
      ps.brokerHost = host;
      ps.brokerId = brokerId;
    }

    private synchronized PartitionStats putPartitionStatsIfNotPresent(KafkaPartition kp)
    {
      PartitionStats ps = partitionStats.get(kp);

      if (ps == null) {
        ps = new PartitionStats();
        partitionStats.put(kp, ps);
      }
      return ps;
    }
  }

  public static class KafkaMessage
  {
    KafkaPartition kafkaPart;
    Message msg;
    long offSet;
    public KafkaMessage(KafkaPartition kafkaPart, Message msg, long offset)
    {
      this.kafkaPart = kafkaPart;
      this.msg = msg;
      this.offSet = offset;
    }

    public KafkaPartition getKafkaPart()
    {
      return kafkaPart;
    }

    public Message getMsg()
    {
      return msg;
    }

    public long getOffSet()
    {
      return offSet;
    }
  }

  public static class KafkaMeterStatsUtil
  {
    public static Map<KafkaPartition, Long> getOffsetsForPartitions(List<KafkaMeterStats> kafkaMeterStats)
    {
      Map<KafkaPartition, Long> result = Maps.newHashMap();
      for (KafkaMeterStats kms : kafkaMeterStats) {
        for (Entry<KafkaPartition, PartitionStats> item : kms.partitionStats.entrySet()) {
          result.put(item.getKey(), item.getValue().offset);
        }
      }
      return result;
    }

    public static Map<KafkaPartition, long[]> get_1minMovingAvgParMap(KafkaMeterStats kafkaMeterStats)
    {
      Map<KafkaPartition, long[]> result = Maps.newHashMap();
      for (Entry<KafkaPartition, PartitionStats> item : kafkaMeterStats.partitionStats.entrySet()) {
        result.put(item.getKey(), new long[]{item.getValue().msgsPerSec, item.getValue().bytesPerSec});
      }
      return result;
    }

  }

  public static class KafkaMeterStatsAggregator implements Context.CountersAggregator, Serializable
  {
    private static final long serialVersionUID = 729987800215151678L;

    @Override
    public Object aggregate(Collection<?> countersList)
    {
      KafkaMeterStats kms = new KafkaMeterStats();
      for (Object o : countersList) {
        if (o instanceof KafkaMeterStats) {
          KafkaMeterStats subKMS = (KafkaMeterStats)o;
          kms.partitionStats.putAll(subKMS.partitionStats);
          kms.totalBytesPerSec += subKMS.totalBytesPerSec;
          kms.totalMsgPerSec += subKMS.totalMsgPerSec;
        }
      }
      return kms;
    }

  }

  public static class PartitionStats implements Serializable
  {
    private static final long serialVersionUID = -6572690643487689766L;

    public int brokerId = -1;

    public long msgsPerSec;

    public long bytesPerSec;

    public long offset;

    public String brokerHost = "";

  }

  /**
   * A snapshot of consuming rate within 1 min
   */
  static class SnapShot
  {
    // msgs/s and bytes/s for each partition

    /**
     * 1 min total msg number for each partition
     */
    private final Map<KafkaPartition, long[]> _1_min_msg_sum_par = new HashMap<KafkaPartition, long[]>();

    /**
     * 1 min total byte number for each partition
     */
    private final Map<KafkaPartition, long[]> _1_min_byte_sum_par = new HashMap<KafkaPartition, long[]>();

    private static int cursor = 0;

    /**
     * total msg for each sec, msgSec[60] is total msg for a min
     */
    private final long[] msgSec = new long[61];

    /**
     * total bytes for each sec, bytesSec[60] is total bytes for a min
     */
    private final long[] bytesSec = new long[61];

    private short last = 1;

    private ScheduledExecutorService service;

    public synchronized void moveNext()
    {
      cursor = (cursor + 1) % 60;
      msgSec[60] -= msgSec[cursor];
      bytesSec[60] -= bytesSec[cursor];
      msgSec[cursor] = 0;
      bytesSec[cursor] = 0;
      for (Entry<KafkaPartition, long[]> item : _1_min_msg_sum_par.entrySet()) {
        long[] msgv = item.getValue();
        long[] bytesv = _1_min_byte_sum_par.get(item.getKey());
        msgv[60] -= msgv[cursor];
        bytesv[60] -= bytesv[cursor];
        msgv[cursor] = 0;
        bytesv[cursor] = 0;
      }

    }


    public void start()
    {
      if (service == null) {
        service = Executors.newScheduledThreadPool(1);
      }
      service.scheduleAtFixedRate(new Runnable()
      {
        @Override
        public void run()
        {
          moveNext();
          if (last < 60) {
            last++;
          }
        }
      }, 1, 1, TimeUnit.SECONDS);

    }

    public void stop()
    {
      if (service != null) {
        service.shutdown();
      }
    }

    public synchronized void mark(KafkaPartition partition, long bytes)
    {
      msgSec[cursor]++;
      msgSec[60]++;
      bytesSec[cursor] += bytes;
      bytesSec[60] += bytes;
      long[] msgv = _1_min_msg_sum_par.get(partition);
      long[] bytev = _1_min_byte_sum_par.get(partition);
      if (msgv == null) {
        msgv = new long[61];
        bytev = new long[61];
        _1_min_msg_sum_par.put(partition, msgv);
        _1_min_byte_sum_par.put(partition, bytev);
      }
      msgv[cursor]++;
      msgv[60]++;
      bytev[cursor] += bytes;
      bytev[60] += bytes;
    }

    public synchronized void setupStats(KafkaMeterStats stat)
    {
      long[] _1minAvg = {msgSec[60] / last, bytesSec[60] / last};
      for (Entry<KafkaPartition, long[]> item : _1_min_msg_sum_par.entrySet()) {
        long[] msgv = item.getValue();
        long[] bytev = _1_min_byte_sum_par.get(item.getKey());
        long[] _1minAvgPar = {msgv[60] / last, bytev[60] / last};
        stat.set_1minMovingAvgPerPartition(item.getKey(), _1minAvgPar);
      }
      stat.set_1minMovingAvg(_1minAvg);
    }
  }

  private SetMultimap<String, String> parseZookeeperStr(String zookeeper)
  {
    SetMultimap<String, String> theClusters = HashMultimap.create();
    for (String zk : zookeeper.split(";")) {
      String[] parts = zk.split("::");
      String clusterId = parts.length == 1 ? KafkaPartition.DEFAULT_CLUSTERID : parts[0];
      theClusters.put(clusterId, parts.length == 1 ? parts[0] : parts[1]);
    }
    return theClusters;
  }
}
