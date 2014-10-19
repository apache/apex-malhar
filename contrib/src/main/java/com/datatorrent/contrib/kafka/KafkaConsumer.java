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
import kafka.message.Message;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Pattern.Flag;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.datatorrent.api.Context;
import com.google.common.collect.Maps;

/**
 * Base Kafka Consumer class used by kafka input operator
 *
 * @since 0.9.0
 */
public abstract class KafkaConsumer implements Closeable
{
  protected final static String HIGHLEVEL_CONSUMER_ID_SUFFIX = "_stream_";

  protected final static String SIMPLE_CONSUMER_ID_SUFFIX = "_partition_";


  public KafkaConsumer()
  {
  }

  public KafkaConsumer(String topic)
  {
    this();
    this.topic = topic;
  }

  public KafkaConsumer(Set<String> brokerSet, String topic)
  {
    this.topic = topic;
    this.brokerSet = brokerSet;
  }

  private int cacheSize = 1024 * 1024;

  protected transient boolean isAlive = false;

  private transient ArrayBlockingQueue<Message> holdingBuffer;

  /**
   * The topic that this consumer consumes
   */
  @NotNull
  protected String topic = "default_topic";

  /**
   * A broker list to retrieve the metadata for the consumer
   * This property could be null
   * But it's mandatory for dynamic partition and fail-over
   */
  @NotNull
  protected Set<String> brokerSet;


  /**
   * The initialOffset could be either earliest or latest
   * Earliest means the beginning the queue
   * Latest means the current sync point to consume the queue
   * This setting is case_insensitive
   * By default it always consume from the beginning of the queue
   */
  @Pattern(flags={Flag.CASE_INSENSITIVE}, regexp = "earliest|latest")
  protected String initialOffset = "latest";


  protected transient SnapShot statsSnapShot = new SnapShot();
  
  protected transient KafkaMeterStats stats = new KafkaMeterStats();

  /**
   * This method is called in setup method of the operator
   */
  public void create(){
    holdingBuffer = new ArrayBlockingQueue<Message>(cacheSize);
  };

  /**
   * This method is called in the activate method of the operator
   */
  public void start(){
    isAlive = true;
    statsSnapShot.start();
  };

  /**
   * The method is called in the deactivate method of the operator
   */
  public void stop() {
    isAlive = false;
    statsSnapShot.stop();
    holdingBuffer.clear();
    IOUtils.closeQuietly(this);
  };

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

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public String getTopic()
  {
    return topic;
  }

  public Message pollMessage()
  {
    return holdingBuffer.poll();
  }

  public int messageSize()
  {
    return holdingBuffer.size();
  }

  public void setBrokerSet(Set<String> brokerSet)
  {
    this.brokerSet = brokerSet;
  }

  public Set<String> getBrokerSet()
  {
    return brokerSet;
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


  final protected void putMessage(int partition, Message msg) throws InterruptedException{
    // block from receiving more message
    holdingBuffer.put(msg);
    statsSnapShot.mark(partition, msg.payloadSize());
  };


  protected abstract KafkaConsumer cloneConsumer(Set<Integer> partitionIds);

  protected abstract KafkaConsumer cloneConsumer(Set<Integer> partitionIds, Map<Integer, Long> startOffset);

  protected abstract void commitOffset();

  protected abstract Map<Integer, Long> getCurrentOffsets();

  public KafkaMeterStats getConsumerStats()
  {
    statsSnapShot.setupStats(stats);
    return stats;
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
    public ConcurrentHashMap<Integer, PartitionStats> partitionStats = new ConcurrentHashMap<Integer, PartitionStats>();

    
    /**
     * A compact overall counter. The data collected is 4bytes connection number + 8bytes aggregate msg/s + 8bytes aggregate bytes/s
     */
    public long totalMsgPerSec;
    
    public long totalBytesPerSec;
    

    public KafkaMeterStats()
    {

    }

    public void set_1minMovingAvgPerPartition(int pid, long[] _1minAvgPar)
    {
      PartitionStats ps = putPartitionStatsIfNotPresent(pid);
      ps.msgsPerSec = _1minAvgPar[0];
      ps.bytesPerSec = _1minAvgPar[1];
    }

    public void set_1minMovingAvg(long[] _1minAvg)
    {
      totalMsgPerSec = _1minAvg[0];
      totalBytesPerSec = _1minAvg[1];
    }
    
    public void updateOffsets(Map<Integer, Long> offsets){
      for (Entry<Integer, Long> os : offsets.entrySet()) {
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

    public void updatePartitionStats(int partitionId,int brokerId, String host)
    {
      PartitionStats ps = putPartitionStatsIfNotPresent(partitionId);
      ps.brokerHost = host;
      ps.brokerId = brokerId;
    }
    
    private synchronized PartitionStats putPartitionStatsIfNotPresent(int pid){
      PartitionStats ps = partitionStats.get(pid);
      if (ps == null) {
        ps = new PartitionStats();
        partitionStats.put(pid, ps);
      }
      return ps;
    }
  }
  
  public static class KafkaMeterStatsUtil {
    
    public static Map<Integer, Long> getOffsetsForPartitions(List<KafkaMeterStats> kafkaMeterStats)
    {
      Map<Integer, Long> result = Maps.newHashMap();
      for (KafkaMeterStats kms : kafkaMeterStats) {
        for (Entry<Integer, PartitionStats> item : kms.partitionStats.entrySet()) {
          result.put(item.getKey(), item.getValue().offset);
        }
      }
      return result;
    }
    
    public static Map<Integer, long[]> get_1minMovingAvgParMap(KafkaMeterStats kafkaMeterStats)
    {
      Map<Integer, long[]> result = Maps.newHashMap();
      for (Entry<Integer, PartitionStats> item : kafkaMeterStats.partitionStats.entrySet()) {
        result.put(item.getKey(), new long[]{item.getValue().msgsPerSec, item.getValue().bytesPerSec});
      }
      return result;
    }
    
  }
  
  public static class KafkaMeterStatsAggregator implements Context.CountersAggregator, Serializable{

    /**
     * 
     */
    private static final long serialVersionUID = 729987800215151678L;

    @Override
    public Object aggregate(Collection<?> countersList)
    {
      KafkaMeterStats kms = new KafkaMeterStats();
      for (Object o : countersList) {
        if (o instanceof KafkaMeterStats){
          KafkaMeterStats subKMS = (KafkaMeterStats)o;
          kms.partitionStats.putAll(subKMS.partitionStats);
          kms.totalBytesPerSec += subKMS.totalBytesPerSec;
          kms.totalMsgPerSec += subKMS.totalMsgPerSec;
        }
      }
      return kms;
    }
    
  }
  
  public static class PartitionStats implements Serializable {


    /**
     * 
     */
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
  static class SnapShot {

    // msgs/s and bytes/s for each partition

    /**
     * 1 min total msg number for each partition
     */
    private final Map<Integer, long[]> _1_min_msg_sum_par = new HashMap<Integer, long[]>();

    /**
     * 1 min total byte number for each partition
     */
    private final Map<Integer, long[]> _1_min_byte_sum_par = new HashMap<Integer, long[]>();

    private static int cursor = 0;

    /**
     * total msg for each sec, msgSec[60] is total msg for a min
     */
    private long[] msgSec = new long[61];

    /**
     * total bytes for each sec, bytesSec[60] is total bytes for a min
     */
    private long[] bytesSec = new long[61];

    private short last = 1;

    private ScheduledExecutorService service;

    public synchronized void moveNext()
    {
      cursor = (cursor + 1) % 60;
      msgSec[60] -= msgSec[cursor];
      bytesSec[60] -= bytesSec[cursor];
      msgSec[cursor] = 0;
      bytesSec[cursor] = 0;
      for (Entry<Integer, long[]> item : _1_min_msg_sum_par.entrySet()) {
        long[] msgv = item.getValue();
        long[] bytesv = _1_min_byte_sum_par.get(item.getKey());
        msgv[60] -= msgv[cursor];
        bytesv[60] -= bytesv[cursor];
        msgv[cursor] = 0;
        bytesv[cursor] = 0;
      }

    }


    public void start(){
      if(service==null){
        service = Executors.newScheduledThreadPool(1);
      }
      service.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run()
        {
          moveNext();
          if(last<60)last++;
        }
      }, 1, 1, TimeUnit.SECONDS);

    }

    public void stop(){
      if(service!=null){
        service.shutdown();
      }
    }

    public synchronized void mark(int partition, long bytes){
      msgSec[cursor]++;
      msgSec[60]++;
      bytesSec[cursor] += bytes;
      bytesSec[60] += bytes;
      long[] msgv = _1_min_msg_sum_par.get(partition);
      long[] bytev = _1_min_byte_sum_par.get(partition);
      if(msgv == null){
        msgv = new long[61];
        bytev = new long[61];
        _1_min_msg_sum_par.put(partition, msgv);
        _1_min_byte_sum_par.put(partition, bytev);
      }
      msgv[cursor]++;
      msgv[60]++;
      bytev[cursor] += bytes;
      bytev[60] += bytes;
    };

    public synchronized void setupStats(KafkaMeterStats stat){
      long[] _1minAvg = {msgSec[60]/last, bytesSec[60]/last};
      for (Entry<Integer, long[]> item : _1_min_msg_sum_par.entrySet()) {
        long[] msgv =item.getValue();
        long[] bytev = _1_min_byte_sum_par.get(item.getKey());
        long[] _1minAvgPar = {msgv[60]/last, bytev[60]/last};
        stat.set_1minMovingAvgPerPartition(item.getKey(), _1minAvgPar);
      }
      stat.set_1minMovingAvg(_1minAvg);
    }
  }

}
