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
package com.datatorrent.contrib.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.*;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.common.util.Pair;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.NotNull;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Base implementation of Kinesis Input Operator. Fetches records from kinesis and emits them as tuples.<br/>
 * <p>
 * <b>Partition Strategy:</b>
 * <p><b>1. ONE_TO_ONE partition</b> Each operator partition will consume from only one Kinesis shard </p>
 * <p><b>2. ONE_TO_MANY partition</b> Each operator partition will consume from more than one kinesis
 *    shard. Dynamic partition is enable by setting the {@link #shardsPerPartition} value > 1</p>
 * <p/>
 * Configurations:<br/>
 * {@link #accessKey} : AWS Credentials AccessKeyId <br/>
 * {@link #secretKey} : AWS Credentials SecretAccessKey <br/>
 * streamName : Name of the stream from where the records to be accessed
 *
 * @param <T>
 * @since 2.0.0
 */
public abstract class AbstractKinesisInputOperator <T> implements InputOperator, ActivationListener<OperatorContext>, Partitioner<AbstractKinesisInputOperator>, StatsListener
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractKinesisInputOperator.class);

  @Min(1)
  private int maxTuplesPerWindow = Integer.MAX_VALUE;
  private int emitCount = 0;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;

  private String endPoint;

  @Valid
  protected KinesisConsumer consumer = new KinesisConsumer();

  // By default the partition policy is 1:1
  public PartitionStrategy strategy = PartitionStrategy.ONE_TO_ONE;

  private transient OperatorContext context = null;

  // Store the current partition info
  private transient Set<PartitionInfo> currentPartitionInfo = new HashSet<PartitionInfo>();

  protected Map<String, String> shardPosition = new HashMap<String, String>();
  private ShardManager shardManager = null;

  // Minimal interval between 2 (re)partition actions
  private long repartitionInterval = 30000L;

  // Minimal interval between checking collected stats and decide whether it needs to repartition or not.
  private long repartitionCheckInterval = 5000L;

  private transient long lastCheckTime = 0L;

  private transient long lastRepartitionTime = 0L;

  //No of shards per partition in dynamic MANY_TO_ONE strategy
  // If the value is more than 1, then it enables the dynamic partitioning
  @Min(1)
  private Integer shardsPerPartition = 1;

  @Min(1)
  private int initialPartitionCount = 1;

  private transient List<String> newWaitingPartition = new LinkedList<String>();

  /**
   * This output port emits tuples extracted from Kinesis data records.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Derived class has to implement this method, so that it knows what type of message it is going to send to Malhar.
   * It converts a ByteBuffer message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param rc Record to convert into tuple
   */
  public abstract T getTuple(Record rc);

  /**
   * Convert the record to tuple and send it to Malhar.
   *
   * @param rc Record to emit
   */
  public void emitTuple(Record rc)
  {
    T tuple = getTuple(rc);
    if(tuple != null)
      outputPort.emit(tuple);
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractKinesisInputOperator>> partitions)
  {
    // update the last repartition time
    lastRepartitionTime = System.currentTimeMillis();
  }

  @Override
  public Collection<Partition<AbstractKinesisInputOperator>> definePartitions(Collection<Partition<AbstractKinesisInputOperator>> partitions, PartitioningContext context)
  {
    boolean isInitialParitition = partitions.iterator().next().getStats() == null;
    // Set the credentials to get the list of shards
    if(isInitialParitition) {
      try {
        KinesisUtil.getInstance().createKinesisClient(accessKey, secretKey, endPoint);
      } catch (Exception e) {
         throw new RuntimeException("[definePartitions]: Unable to load credentials. ", e);
      }
    }
    List<Shard> shards = KinesisUtil.getInstance().getShardList(getStreamName());

    // Operator partitions
    List<Partition<AbstractKinesisInputOperator>> newPartitions = null;

    // initialize the shard positions
    Map<String, String> initShardPos = null;
    if(isInitialParitition && shardManager !=null){
      initShardPos = shardManager.loadInitialShardPositions();
    }

    switch (strategy) {
    // For the 1 to 1 mapping The framework will create number of operator partitions based on kinesis shards
    // Each operator partition will consume from only one kinesis shard
    case ONE_TO_ONE:
      if (isInitialParitition) {
        lastRepartitionTime = System.currentTimeMillis();
        logger.info("[ONE_TO_ONE]: Initializing partition(s)");
        // initialize the number of operator partitions according to number of shards
        newPartitions = new ArrayList<Partition<AbstractKinesisInputOperator>>(shards.size());
        for (int i = 0; i < shards.size(); i++) {
          logger.info("[ONE_TO_ONE]: Create operator partition for kinesis partition: " + shards.get(i).getShardId() + ", StreamName: " + this.getConsumer().streamName);
          newPartitions.add(createPartition(Sets.newHashSet(shards.get(i).getShardId()), initShardPos));
        }
      } else if (newWaitingPartition.size() != 0) {
        // Remove the partitions for the closed shards
        removePartitionsForClosedShards(partitions);
        // add partition for new kinesis shard
        for (String pid : newWaitingPartition) {
          logger.info("[ONE_TO_ONE]: Add operator partition for kinesis partition " + pid);
          partitions.add(createPartition(Sets.newHashSet(pid), null));
        }
        newWaitingPartition.clear();
        return partitions;
      }
      break;
    // For the N to 1 mapping The initial partition number is defined by stream application
    // Afterwards, the framework will dynamically adjust the partition
    case MANY_TO_ONE:
      /* This case was handled into two ways.
         1. Dynamic Partition: Number of DT partitions is depends on the number of open shards.
         2. Static Partition: Number of DT partitions is fixed, whether the number of shards are increased/decreased.
      */
      int size = initialPartitionCount;
      if(newWaitingPartition.size() != 0)
      {
        // Get the list of open shards
        shards = getOpenShards(partitions);
        if(shardsPerPartition > 1)
          size = (int)Math.ceil(shards.size() / (shardsPerPartition * 1.0));
        initShardPos = shardManager.loadInitialShardPositions();
      }
      Set<String>[] pIds = (Set<String>[]) Array.newInstance((new HashSet<String>()).getClass(), size);

      newPartitions = new ArrayList<Partition<AbstractKinesisInputOperator>>(size);
      for (int i = 0; i < shards.size(); i++) {
        Shard pm = shards.get(i);
        if (pIds[i % size] == null) {
          pIds[i % size] = new HashSet<String>();
        }
        pIds[i % size].add(pm.getShardId());
      }
      if (isInitialParitition) {
        lastRepartitionTime = System.currentTimeMillis();
        logger.info("[MANY_TO_ONE]: Initializing partition(s)");
      } else {
        logger.info("[MANY_TO_ONE]: Add operator partition for kinesis partition(s): " + StringUtils.join(newWaitingPartition, ", ") + ", StreamName: " + this.getConsumer().streamName);
        newWaitingPartition.clear();
      }
      for (int i = 0; i < pIds.length; i++) {
        logger.info("[MANY_TO_ONE]: Create operator partition for kinesis partition(s): " + StringUtils.join(pIds[i], ", ") + ", StreamName: " + this.getConsumer().streamName);
        if(pIds[i] != null)
          newPartitions.add(createPartition(pIds[i], initShardPos));
      }
      break;
    default:
      break;
    }
    return newPartitions;
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    Response resp = new Response();
    List<KinesisConsumer.KinesisShardStats> kstats = extractkinesisStats(stats);
    resp.repartitionRequired = isPartitionRequired(stats.getOperatorId(), kstats);
    return resp;
  }

  private void updateShardPositions(List<KinesisConsumer.KinesisShardStats> kstats)
  {
    //In every partition check interval, call shardmanager to update the positions
    if (shardManager != null) {
      shardManager.updatePositions(KinesisConsumer.KinesisShardStatsUtil.getShardStatsForPartitions(kstats));
    }
  }

  private List<KinesisConsumer.KinesisShardStats> extractkinesisStats(BatchedOperatorStats stats)
  {
    //preprocess the stats
    List<KinesisConsumer.KinesisShardStats> kmsList = new LinkedList<KinesisConsumer.KinesisShardStats>();
    for (Stats.OperatorStats os : stats.getLastWindowedStats()) {
      if (os != null && os.counters instanceof KinesisConsumer.KinesisShardStats) {
        kmsList.add((KinesisConsumer.KinesisShardStats) os.counters);
      }
    }
    return kmsList;
  }

  private boolean isPartitionRequired(int opid,  List<KinesisConsumer.KinesisShardStats> kstats)
  {

    long t = System.currentTimeMillis();

    if (t - lastCheckTime < repartitionCheckInterval) {
      // return false if it's within repartitionCheckInterval since last time it check the stats
      return false;
    }
    logger.debug("Use ShardManager to update the Shard Positions");
    updateShardPositions(kstats);
    if(repartitionInterval < 0){
      // if repartition is disabled
      return false;
    }

    if(t - lastRepartitionTime < repartitionInterval) {
      // return false if it's still within repartitionInterval since last (re)partition
      return false;
    }

    try {
      // monitor if shards are repartitioned
      Set<String> existingIds = new HashSet<String>();
      for (PartitionInfo pio : currentPartitionInfo) {
        existingIds.addAll(pio.kpids);
      }
      List<Shard> shards = KinesisUtil.getInstance().getShardList(getStreamName());
      for (Shard shard :shards) {
        if (!existingIds.contains(shard.getShardId())) {
          newWaitingPartition.add(shard.getShardId());
        }
      }

      if (newWaitingPartition.size() != 0) {
        // found new kinesis partition
        lastRepartitionTime = t;
        return true;
      }
      return false;
    } finally {
      // update last  check time
      lastCheckTime = System.currentTimeMillis();
    }
  }

  // If all the shards in the partition are closed, then remove that partition
  private void removePartitionsForClosedShards(Collection<Partition<AbstractKinesisInputOperator>> partitions)
  {
    List<Partition<AbstractKinesisInputOperator>> closedPartitions = new ArrayList<Partition<AbstractKinesisInputOperator>>();
    for(Partition<AbstractKinesisInputOperator> op : partitions)
    {
      if(op.getPartitionedInstance().getConsumer().getClosedShards().size() ==
          op.getPartitionedInstance().getConsumer().getNumOfShards())
      {
        closedPartitions.add(op);
      }
    }
    if(closedPartitions.size() != 0)
    {
      for(Partition<AbstractKinesisInputOperator> op : closedPartitions)
      {
        partitions.remove(op);
      }
    }
  }

  // Get the list of open shards
  private List<Shard> getOpenShards(Collection<Partition<AbstractKinesisInputOperator>> partitions)
  {
    List<Shard> closedShards = new ArrayList<Shard>();
    for(Partition<AbstractKinesisInputOperator> op : partitions)
    {
      closedShards.addAll(op.getPartitionedInstance().getConsumer().getClosedShards());
    }
    List<Shard> shards = KinesisUtil.getInstance().getShardList(getStreamName());
    List<Shard> openShards = new ArrayList<Shard>();
    for (Shard shard :shards) {
      if(!closedShards.contains(shard)) {
        openShards.add(shard);
      }
    }
    return openShards;
  }
  // Create a new partition with the shardIds and initial shard positions
  private
  Partition<AbstractKinesisInputOperator> createPartition(Set<String> shardIds, Map<String, String> initShardPos)
  {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    kryo.writeObject(output, this);
    output.close();
    Input lInput = new Input(bos.toByteArray());
    Partition<AbstractKinesisInputOperator> p = new DefaultPartition<AbstractKinesisInputOperator>(kryo.readObject(lInput, this.getClass()));
    p.getPartitionedInstance().getConsumer().setShardIds(shardIds);
    p.getPartitionedInstance().getConsumer().resetShardPositions(initShardPos);

    PartitionInfo pif = new PartitionInfo();
    pif.kpids = shardIds;
    currentPartitionInfo.add(pif);
    return p;
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    this.context = context;
    try {
      KinesisUtil.getInstance().createKinesisClient(accessKey, secretKey, endPoint);
    } catch(Exception e)
    {
      throw new RuntimeException(e);
    }
    consumer.create();
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    consumer.teardown();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    emitCount = 0;
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    context.setCounters(getConsumer().getConsumerStats(shardPosition));
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void activate(OperatorContext ctx)
  {
    consumer.start();
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void deactivate()
  {
    consumer.stop();
  }

  /**
   * Implement InputOperator Interface.
   */
  @Override
  public void emitTuples()
  {
    int count = consumer.getQueueSize();
    if(maxTuplesPerWindow > 0)
      count = Math.min(count, maxTuplesPerWindow - emitCount);
    for (int i = 0; i < count; i++) {
      Pair<String, Record> data = consumer.pollRecord();
      emitTuple(data.getSecond());
      shardPosition.put(data.getFirst(), data.getSecond().getSequenceNumber());
    }
    emitCount += count;
  }

  public static enum PartitionStrategy
  {
    /**
     * Each operator partition connect to only one kinesis Shard
     */
    ONE_TO_ONE,
    /**
     * Each operator consumes from several Shards.
     */
    MANY_TO_ONE
  }

  static class PartitionInfo
  {
    Set<String> kpids;
  }

  public void setConsumer(KinesisConsumer consumer)
  {
    this.consumer = consumer;
  }

  public KinesisConsumer getConsumer()
  {
    return consumer;
  }

  public String getStreamName()
  {
    return this.consumer.getStreamName();
  }

  public void setStreamName(String streamName)
  {
    this.consumer.setStreamName(streamName);
  }

  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  public PartitionStrategy getStrategy()
  {
    return strategy;
  }

  public void setStrategy(PartitionStrategy strategy)
  {
    this.strategy = strategy;
  }

  public void setStrategy(String policy)
  {
    this.strategy = PartitionStrategy.valueOf(policy.toUpperCase());
  }

  public OperatorContext getContext()
  {
    return context;
  }

  public void setContext(OperatorContext context)
  {
    this.context = context;
  }

  public ShardManager getShardManager()
  {
    return shardManager;
  }

  public void setShardManager(ShardManager shardManager)
  {
    this.shardManager = shardManager;
  }

  public long getRepartitionInterval()
  {
    return repartitionInterval;
  }

  public void setRepartitionInterval(long repartitionInterval)
  {
    this.repartitionInterval = repartitionInterval;
  }

  public long getRepartitionCheckInterval()
  {
    return repartitionCheckInterval;
  }

  public void setRepartitionCheckInterval(long repartitionCheckInterval)
  {
    this.repartitionCheckInterval = repartitionCheckInterval;
  }

  public Integer getShardsPerPartition()
  {
    return shardsPerPartition;
  }

  public void setShardsPerPartition(Integer shardsPerPartition)
  {
    this.shardsPerPartition = shardsPerPartition;
  }

  public int getInitialPartitionCount()
  {
    return initialPartitionCount;
  }

  public void setInitialPartitionCount(int initialPartitionCount)
  {
    this.initialPartitionCount = initialPartitionCount;
  }

  public void setInitialOffset(String initialOffset)
  {
    this.consumer.initialOffset = initialOffset;
  }

  public String getAccessKey()
  {
    return accessKey;
  }

  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  public String getSecretKey()
  {
    return secretKey;
  }

  public void setSecretKey(String secretKey)
  {
    this.secretKey = secretKey;
  }

  public String getEndPoint()
  {
    return endPoint;
  }

  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }
}
