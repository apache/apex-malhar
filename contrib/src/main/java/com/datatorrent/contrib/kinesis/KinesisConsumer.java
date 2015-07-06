/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kinesis;

import java.io.Closeable;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Pattern.Flag;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;

import com.datatorrent.common.util.Pair;

/**
 *
 * Base KinesisConsumer class used by kinesis input operator
 * @since 2.0.0
 */

public class KinesisConsumer implements Closeable
{
  private static final Logger logger = LoggerFactory.getLogger(KinesisConsumer.class);

  protected Integer recordsLimit = 100;

  private int bufferSize = 1024;

  protected transient boolean isAlive = false;

  private transient ArrayBlockingQueue<Pair<String,Record>> holdingBuffer;

  /**
   * The streamName that this consumer consumes
   */
  @NotNull
  protected String streamName;

  /**
   * The initialOffset could be either earliest or latest
   * Earliest means the beginning the shard
   * Latest means the current record to consume from the shard
   * By default it always consume from the beginning of the shard
   */
  @Pattern(flags={Flag.CASE_INSENSITIVE}, regexp = "earliest|latest")
  protected String initialOffset = "latest";

  protected transient ExecutorService consumerThreadExecutor = null;

  protected ConcurrentHashMap<String, String> shardPosition = new ConcurrentHashMap<String, String>();
  protected final transient HashSet<Shard> simpleConsumerThreads = new HashSet<Shard>();
  private Set<String> shardIds = new HashSet<String>();
  protected Set<Shard> closedShards = new HashSet<Shard>();
  protected long recordsCheckInterval = 500;


  protected transient KinesisShardStats stats = new KinesisShardStats();

  public KinesisConsumer()
  {
  }

  public KinesisConsumer(String streamName)
  {
    this.streamName = streamName;
  }

  public KinesisConsumer(String streamName, Set<String> newShardIds)
  {
    this(streamName);
    shardIds = newShardIds;
  }
  /**
   * This method is called in setup method of the operator
   */
  public void create(){
    holdingBuffer = new ArrayBlockingQueue<Pair<String, Record>>(bufferSize);
    boolean defaultSelect = (shardIds == null) || (shardIds.size() == 0);
    final List<Shard> pms = KinesisUtil.getInstance().getShardList(streamName);
    for (final Shard shId: pms) {
      if((shardIds.contains(shId.getShardId()) || defaultSelect) && !closedShards.contains(shId)) {
        simpleConsumerThreads.add(shId);
      }
    }
  }

  /**
   * This method returns the iterator type of the given shard
   */
  public ShardIteratorType getIteratorType(String shardId)
  {
    if(shardPosition.containsKey(shardId)) {
      return ShardIteratorType.AFTER_SEQUENCE_NUMBER;
    }
    return initialOffset.equalsIgnoreCase("earliest") ? ShardIteratorType.TRIM_HORIZON : ShardIteratorType.LATEST;
  }

  /**
   * This method is called in the activate method of the operator
   */
  public void start(){
    isAlive = true;
    int realNumStream =  simpleConsumerThreads.size();
    if(realNumStream == 0)
      return;

    consumerThreadExecutor = Executors.newFixedThreadPool(realNumStream);
    for (final Shard shd : simpleConsumerThreads) {
      consumerThreadExecutor.submit(new Runnable() {
        @Override
        public void run()
        {
          logger.debug("Thread " + Thread.currentThread().getName() + " start consuming Records...");
          while (isAlive ) {
            Shard shard = shd;
            try {
              List<Record> records = KinesisUtil.getInstance().getRecords(streamName, recordsLimit,
                  shard.getShardId(), getIteratorType(shard.getShardId()), shardPosition.get(shard.getShardId()));

              if (records == null || records.isEmpty()) {
                if (shard.getSequenceNumberRange().getEndingSequenceNumber() != null) {
                  closedShards.add(shard);
                  break;
                }
                try {
                  Thread.sleep(recordsCheckInterval);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              } else {
                String seqNo = "";
                for (Record rc : records) {
                    seqNo = rc.getSequenceNumber();
                    putRecord(shd.getShardId(), rc);
                }
                shardPosition.put(shard.getShardId(), seqNo);
              }
            } catch(Exception e)
            {
              throw new RuntimeException(e);
            }
          }
          logger.debug("Thread " + Thread.currentThread().getName() + " stop consuming Records...");
        }
      });
    }
  }

  @Override
  public void close()
  {
    if(consumerThreadExecutor!=null) {
      consumerThreadExecutor.shutdown();
    }
    simpleConsumerThreads.clear();
  }

  /**
   * The method is called in the deactivate method of the operator
   */
  public void stop() {
    isAlive = false;
    holdingBuffer.clear();
    IOUtils.closeQuietly(this);
  }

  public void resetShardPositions(Map<String, String> shardPositions){

    if(shardPositions == null){
      return;
    }
    shardPosition.clear();

    for (String pid: shardIds) {
      String offsetForPar = shardPositions.get(pid);
      if (offsetForPar != null && !offsetForPar.equals("")) {
        shardPosition.put(pid, offsetForPar);
      }
    }
  }

  protected Map<String, String> getShardPosition()
  {
    return shardPosition;
  }

  public Set<Shard> getClosedShards()
  {
    return closedShards;
  }

  public Integer getNumOfShards()
  {
    return shardIds.size();
  }

  public KinesisShardStats getConsumerStats(Map<String, String> shardStats)
  {
    stats.updateShardStats(shardStats);
    return stats;
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

  public void setStreamName(String streamName)
  {
    this.streamName = streamName;
  }

  public String getStreamName()
  {
    return streamName;
  }

  public Pair<String, Record> pollRecord()
  {
    return holdingBuffer.poll();
  }

  public int getQueueSize()
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

  final protected void putRecord(String shardId, Record msg) throws InterruptedException{
    holdingBuffer.put(new Pair<String, Record>(shardId, msg));
  };

  public Integer getRecordsLimit()
  {
    return recordsLimit;
  }

  public void setRecordsLimit(Integer recordsLimit)
  {
    this.recordsLimit = recordsLimit;
  }

  public Set<String> getShardIds()
  {
    return shardIds;
  }

  public void setShardIds(Set<String> shardIds)
  {
    this.shardIds = shardIds;
  }

  public void setQueueSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }


  /**
   * Counter class which gives the statistic value from the consumer
   */
  @SuppressWarnings("serial")
  public static class KinesisShardStats implements Serializable
  {
    public ConcurrentHashMap<String, String> partitionStats = new ConcurrentHashMap<String, String>();

    public KinesisShardStats()
    {
    }
    //important API for update
    public void updateShardStats(Map<String, String> shardStats){
      for (Entry<String, String> ss : shardStats.entrySet()) {
        partitionStats.put(ss.getKey(), ss.getValue());
      }
    }
  }

  public static class KinesisShardStatsUtil {
    public static Map<String, String> getShardStatsForPartitions(List<KinesisShardStats> kinesisshardStats)
    {
      Map<String, String> result = Maps.newHashMap();
      for (KinesisShardStats kms : kinesisshardStats) {
        for (Entry<String, String> item : kms.partitionStats.entrySet()) {
          result.put(item.getKey(), item.getValue());
        }
      }
      return result;
    }
  }
}
