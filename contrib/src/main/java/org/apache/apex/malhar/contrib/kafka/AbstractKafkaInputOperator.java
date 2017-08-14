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

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import static org.apache.apex.malhar.contrib.kafka.KafkaConsumer.KafkaMeterStatsUtil.getOffsetsForPartitions;

/**
 * This is a base implementation of a Kafka input operator, which consumes data from Kafka message bus.&nbsp;
 * Subclasses should implement the method for emitting tuples to downstream operators.
 * It will be dynamically partitioned based on the upstream kafka partition.
 * <p>
 * <b>Partition Strategy:</b>
 * <p><b>1. ONE_TO_ONE partition</b> Each operator partition will consume from only one kafka partition </p>
 * <p><b>2. ONE_TO_MANY partition</b> Each operator partition consumer from multiple kafka partition with some hard ingestion rate limit</p>
 * <p><b>3. ONE_TO_MANY_HEURISTIC partition</b>(Not implemented yet) Each operator partition consumer from multiple kafka partition and partition number depends on heuristic function(real time bottle neck)</p>
 * <p><b>Note:</b> ONE_TO_MANY partition only support simple kafka consumer because
 * <p>  1) high-level consumer can only balance the number of brokers it consumes from rather than the actual load from each broker</p>
 * <p>  2) high-level consumer can not reset offset once it's committed so the tuples are not replayable </p>
 * <p></p>
 * <br>
 * <br>
 * <b>Basic Algorithm:</b>
 * <p>1.Pull the metadata(how many partitions) of the topic from brokerList of {@link KafkaConsumer}</p>
 * <p>2.cloneConsumer method is used to initialize the new {@link KafkaConsumer} instance for the new partition operator</p>
 * <p>3.cloneOperator method is used to initialize the new {@link AbstractKafkaInputOperator} instance for the new partition operator</p>
 * <p>4.ONE_TO_MANY partition use first-fit decreasing algorithm(http://en.wikipedia.org/wiki/Bin_packing_problem) to minimize the partition operator
 * <br>
 * <br>
 * <b>Load balance:</b> refer to {@link SimpleKafkaConsumer} and {@link HighlevelKafkaConsumer} <br>
 * <b>Kafka partition failover:</b> refer to {@link SimpleKafkaConsumer} and {@link HighlevelKafkaConsumer}
 * <br>
 * <br>
 * <b>Self adjust to Kafka partition change:</b>
 * <p><b>EACH</b> operator partition periodically check the leader broker(s) change which it consumes from and adjust connection without repartition</p>
 * <p><b>ONLY APPMASTER</b> operator periodically check overall kafka partition layout and add operator partition due to kafka partition add(no delete supported by kafka for now)</p>
 * <br>
 * <br>
 * </p>
 * Properties:<br>
 * <b>tuplesBlast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 *
 * Each operator can consume 1 topic from multiple partitions and clusters<br>
 * </p>
 *
 * @displayName Abstract Kafka Input
 * @category Messaging
 * @tags input operator
 *
 * @since 0.3.2
 */

@OperatorAnnotation(partitionable = true)
public abstract class AbstractKafkaInputOperator<K extends KafkaConsumer> implements InputOperator, ActivationListener<OperatorContext>, Operator.CheckpointNotificationListener, Partitioner<AbstractKafkaInputOperator<K>>, StatsListener
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaInputOperator.class);

  @Min(1)
  private int maxTuplesPerWindow = Integer.MAX_VALUE;
  @Min(1)
  private long maxTotalMsgSizePerWindow = Long.MAX_VALUE;
  private transient int emitCount = 0;
  private transient long emitTotalMsgSize = 0;
  protected WindowDataManager windowDataManager;
  protected transient long currentWindowId;
  protected transient int operatorId;
  protected final transient Map<KafkaPartition, MutablePair<Long, Integer>> currentWindowRecoveryState;
  /**
   * Offsets that are checkpointed for recovery
   */
  protected Map<KafkaPartition, Long> offsetStats = new HashMap<KafkaPartition, Long>();
  /**
   * offset history with window id
   */
  protected transient List<Pair<Long, Map<KafkaPartition, Long>>> offsetTrackHistory = new LinkedList<>();
  private transient OperatorContext context = null;
  // By default the partition policy is 1:1
  public PartitionStrategy strategy = PartitionStrategy.ONE_TO_ONE;

  // Deprecated: Please don't use this property.
  @Deprecated
  private long msgRateUpperBound = Long.MAX_VALUE;

  // Deprecated: Please don't use this property.
  @Deprecated
  private long byteRateUpperBound = Long.MAX_VALUE;

  // Store the current operator partition topology
  private transient List<PartitionInfo> currentPartitionInfo = Lists.newLinkedList();

  // Store the current collected kafka consumer stats
  private transient Map<Integer, List<KafkaConsumer.KafkaMeterStats>> kafkaStatsHolder = new HashMap<Integer, List<KafkaConsumer.KafkaMeterStats>>();

  private OffsetManager offsetManager = null;

  // Minimal interval between 2 (re)partition actions
  private long repartitionInterval = 30000L;

  // Minimal interval between checking collected stats and decide whether it needs to repartition or not.
  // And minimal interval between 2 offset updates
  private long repartitionCheckInterval = 5000L;

  private transient long lastCheckTime = 0L;

  private transient long lastRepartitionTime = 0L;

  // A list store the newly discovered partitions
  private transient List<KafkaPartition> newWaitingPartition = new LinkedList<KafkaPartition>();

  private transient KafkaConsumer.KafkaMessage pendingMessage;

  @Min(1)
  private int initialPartitionCount = 1;

  @NotNull
  @Valid
  protected KafkaConsumer consumer = new SimpleKafkaConsumer();

  public AbstractKafkaInputOperator()
  {
    windowDataManager = new WindowDataManager.NoopWindowDataManager();
    currentWindowRecoveryState = new HashMap<KafkaPartition, MutablePair<Long, Integer>>();
  }

  /**
   * Any concrete class derived from KafkaInputOperator has to implement this method to emit tuples to an output port.
   *
   */
  protected abstract void emitTuple(Message message);

  /**
   * Concrete class derived from KafkaInputOpertor should implement this method if it wants to access kafka offset and partitionId along with kafka message.
   */
  protected void emitTuple(KafkaConsumer.KafkaMessage message)
  {
    emitTuple(message.msg);
  }

  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  /**
   * Get the maximum total size of messages to be transmitted per window. When the sum of the message sizes transmitted
   * in a window reaches this limit no more messages are transmitted till the next window. There is one exception
   * however, if the size of the first message in a window is greater than the limit it is still transmitted so that the
   * processing of messages doesn't get stuck.
   * @return The maximum for the total size
     */
  public long getMaxTotalMsgSizePerWindow()
  {
    return maxTotalMsgSizePerWindow;
  }

  /**
   * Set the maximum total size of messages to be transmitted per window. See {@link #getMaxTotalMsgSizePerWindow()} for
   * more description about this property.
   *
   * @param maxTotalMsgSizePerWindow The maximum for the total size
     */
  public void setMaxTotalMsgSizePerWindow(long maxTotalMsgSizePerWindow)
  {
    this.maxTotalMsgSizePerWindow = maxTotalMsgSizePerWindow;
  }

  @Override
  public void setup(OperatorContext context)
  {
    logger.debug("consumer {} topic {} cacheSize {}", consumer, consumer.getTopic(), consumer.getCacheSize());
    consumer.create();
    // reset the offsets to checkpointed one
    if (consumer instanceof SimpleKafkaConsumer && !offsetStats.isEmpty()) {
      Map<KafkaPartition, Long> currentOffsets = new HashMap<>();
      // Increment the offsets and set it to consumer
      for (Map.Entry<KafkaPartition, Long> e: offsetStats.entrySet()) {
        currentOffsets.put(e.getKey(), e.getValue() + 1);
      }
      ((SimpleKafkaConsumer)consumer).resetOffset(currentOffsets);
    }
    this.context = context;
    operatorId = context.getId();
    if (consumer instanceof HighlevelKafkaConsumer && !(windowDataManager instanceof WindowDataManager.NoopWindowDataManager)) {
      throw new RuntimeException("Idempotency is not supported for High Level Kafka Consumer");
    }
    windowDataManager.setup(context);
  }

  @Override
  public void teardown()
  {
    windowDataManager.teardown();
    consumer.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (windowId <= windowDataManager.getLargestCompletedWindow()) {
      replay(windowId);
    }
    emitCount = 0;
    emitTotalMsgSize = 0;
  }

  protected void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      Map<KafkaPartition, MutablePair<Long, Integer>> recoveredData = (Map<KafkaPartition, MutablePair<Long, Integer>>)
          windowDataManager.retrieve(windowId);
      if (recoveredData != null) {
        Map<String, List<PartitionMetadata>> pms = KafkaMetadataUtil.getPartitionsForTopic(getConsumer().brokers, getConsumer().topic);
        if (pms != null) {
          SimpleKafkaConsumer cons = (SimpleKafkaConsumer)getConsumer();
          // add all partition request in one Fretch request together
          FetchRequestBuilder frb = new FetchRequestBuilder().clientId(cons.getClientId());
          for (Map.Entry<KafkaPartition, MutablePair<Long, Integer>> rc : recoveredData.entrySet()) {
            KafkaPartition kp = rc.getKey();
            List<PartitionMetadata> pmsVal = pms.get(kp.getClusterId());

            Iterator<PartitionMetadata> pmIterator = pmsVal.iterator();
            PartitionMetadata pm = pmIterator.next();
            while (pm.partitionId() != kp.getPartitionId()) {
              if (!pmIterator.hasNext()) {
                break;
              }
              pm = pmIterator.next();
            }
            if (pm.partitionId() != kp.getPartitionId()) {
              continue;
            }
            Broker bk = pm.leader();

            frb.addFetch(consumer.topic, rc.getKey().getPartitionId(), rc.getValue().left, cons.getBufferSize());
            FetchRequest req = frb.build();

            SimpleConsumer ksc = new SimpleConsumer(bk.host(), bk.port(), cons.getTimeout(), cons.getBufferSize(), cons.getClientId());
            FetchResponse fetchResponse = ksc.fetch(req);
            Integer count = 0;
            for (MessageAndOffset msg : fetchResponse.messageSet(consumer.topic, kp.getPartitionId())) {
              KafkaConsumer.KafkaMessage kafkaMessage = new KafkaConsumer.KafkaMessage(kp, msg.message(), msg.offset());
              emitTuple(kafkaMessage);
              offsetStats.put(kp, msg.offset());
              count = count + 1;
              if (count.equals(rc.getValue().right)) {
                break;
              }
            }
          }
        }
      }
      if (windowId == windowDataManager.getLargestCompletedWindow()) {
        // Start the consumer at the largest recovery window
        SimpleKafkaConsumer cons = (SimpleKafkaConsumer)getConsumer();
        // Set the offset positions to the consumer
        Map<KafkaPartition, Long> currentOffsets = new HashMap<KafkaPartition, Long>(cons.getCurrentOffsets());
        // Increment the offsets
        for (Map.Entry<KafkaPartition, Long> e: offsetStats.entrySet()) {
          currentOffsets.put(e.getKey(), e.getValue() + 1);
        }
        cons.resetOffset(currentOffsets);
        cons.start();
      }
    } catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
  }

  @Override
  public void endWindow()
  {
    //TODO depends on APEX-78 only needs to keep the history of windows needs to be commit
    if (getConsumer() instanceof SimpleKafkaConsumer) {
      Map<KafkaPartition, Long> carryOn = new HashMap<>(offsetStats);
      offsetTrackHistory.add(Pair.of(currentWindowId, carryOn));
    }
    if (currentWindowId > windowDataManager.getLargestCompletedWindow()) {
      try {
        windowDataManager.save(currentWindowRecoveryState, currentWindowId);
      } catch (IOException e) {
        throw new RuntimeException("saving recovery", e);
      }
    }
    currentWindowRecoveryState.clear();
  }

  @Override
  public void checkpointed(long windowId)
  {
    // commit the consumer offset
    getConsumer().commitOffset();
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {

  }

  @Override
  public void committed(long windowId)
  {
    if ((getConsumer() instanceof  SimpleKafkaConsumer)) {
      SimpleKafkaConsumer cons = (SimpleKafkaConsumer)getConsumer();
      for (Iterator<Pair<Long, Map<KafkaPartition, Long>>> iter = offsetTrackHistory.iterator(); iter.hasNext(); ) {
        Pair<Long, Map<KafkaPartition, Long>> item = iter.next();
        if (item.getLeft() < windowId) {
          iter.remove();
          continue;
        } else if (item.getLeft() == windowId) {
          if (logger.isDebugEnabled()) {
            logger.debug("report offsets {} ", Joiner.on(';').withKeyValueSeparator("=").join(item.getRight()));
          }
          context.setCounters(cons.getConsumerStats(item.getRight()));
        }
        break;
      }
    }

    try {
      windowDataManager.committed(windowId);
    } catch (IOException e) {
      throw new RuntimeException("deleting state", e);
    }
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    if (context.getValue(OperatorContext.ACTIVATION_WINDOW_ID) != Stateless.WINDOW_ID &&
        context.getValue(OperatorContext.ACTIVATION_WINDOW_ID) < windowDataManager.getLargestCompletedWindow()) {
      // If it is a replay state, don't start the consumer
      return;
    }
    // Don't start thread here!
    // # of kafka_consumer_threads depends on the type of kafka client and the message
    // metadata(topic/partition/replica) layout
    consumer.start();
  }

  @Override
  public void deactivate()
  {
    consumer.stop();
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= windowDataManager.getLargestCompletedWindow()) {
      return;
    }
    int count = consumer.messageSize() + ((pendingMessage != null) ? 1 : 0);
    if (maxTuplesPerWindow > 0) {
      count = Math.min(count, maxTuplesPerWindow - emitCount);
    }
    KafkaConsumer.KafkaMessage message = null;
    for (int i = 0; i < count; i++) {
      if (pendingMessage != null) {
        message = pendingMessage;
        pendingMessage = null;
      } else {
        message = consumer.pollMessage();
      }
      // If the total size transmitted in the window will be exceeded don't transmit anymore messages in this window
      // Make an exception for the case when no message has been transmitted in the window and transmit at least one
      // message even if the condition is violated so that the processing doesn't get stuck
      if ((emitCount > 0) && ((maxTotalMsgSizePerWindow - emitTotalMsgSize) < message.msg.size())) {
        pendingMessage = message;
        break;
      }
      emitTuple(message);
      emitCount++;
      emitTotalMsgSize += message.msg.size();
      offsetStats.put(message.kafkaPart, message.offSet);
      MutablePair<Long, Integer> offsetAndCount = currentWindowRecoveryState.get(message.kafkaPart);
      if (offsetAndCount == null) {
        currentWindowRecoveryState.put(message.kafkaPart, new MutablePair<Long, Integer>(message.offSet, 1));
      } else {
        offsetAndCount.setRight(offsetAndCount.right + 1);
      }
    }
  }

  public void setConsumer(K consumer)
  {
    this.consumer = consumer;
  }

  public KafkaConsumer getConsumer()
  {
    return consumer;
  }

  /**
   * Set the Topic.
   * @omitFromUI
   */
  @Deprecated
  public void setTopic(String topic)
  {
    this.consumer.setTopic(topic);
  }

  /**
   * Set the ZooKeeper quorum of the Kafka cluster(s) you want to consume data from.
   * The operator will discover the brokers that it needs to consume messages from.
   * @omitFromUI
   */
  @Deprecated
  public void setZookeeper(String zookeeperString)
  {
    this.consumer.setZookeeper(zookeeperString);
  }

  @Override
  public void partitioned(Map<Integer, Partitioner.Partition<AbstractKafkaInputOperator<K>>> partitions)
  {
    // update the last repartition time
    lastRepartitionTime = System.currentTimeMillis();
  }

  @Override
  public Collection<Partitioner.Partition<AbstractKafkaInputOperator<K>>> definePartitions(Collection<Partitioner.Partition<AbstractKafkaInputOperator<K>>> partitions, Partitioner.PartitioningContext context)
  {
    // Initialize brokers from zookeepers
    getConsumer().initBrokers();

    boolean isInitialParitition = true;
    // check if it's the initial partition
    if (partitions.iterator().hasNext()) {
      isInitialParitition = partitions.iterator().next().getStats() == null;
    }

    // Operator partitions
    List<Partitioner.Partition<AbstractKafkaInputOperator<K>>> newPartitions = null;

    // initialize the offset
    Map<KafkaPartition, Long> initOffset = null;
    if (isInitialParitition && offsetManager != null) {
      initOffset = offsetManager.loadInitialOffsets();
      logger.info("Initial offsets: {} ", "{ " + Joiner.on(", ").useForNull("").withKeyValueSeparator(": ").join(initOffset) + " }");
    }

    Set<Integer> deletedOperators = Sets.newHashSet();
    Collection<Partition<AbstractKafkaInputOperator<K>>> resultPartitions = partitions;
    boolean numPartitionsChanged = false;

    switch (strategy) {

      // For the 1 to 1 mapping The framework will create number of operator partitions based on kafka topic partitions
      // Each operator partition will consume from only one kafka partition
      case ONE_TO_ONE:

        if (isInitialParitition) {
          lastRepartitionTime = System.currentTimeMillis();
          logger.info("[ONE_TO_ONE]: Initializing partition(s)");
          // get partition metadata for topics.
          // Whatever operator is using high-level or simple kafka consumer, the operator always create a temporary simple kafka consumer to get the metadata of the topic
          // The initial value of brokerList of the KafkaConsumer is used to retrieve the topic metadata
          Map<String, List<PartitionMetadata>> kafkaPartitions = KafkaMetadataUtil.getPartitionsForTopic(getConsumer().brokers, getConsumer().getTopic());

          // initialize the number of operator partitions according to number of kafka partitions

          newPartitions = new LinkedList<Partitioner.Partition<AbstractKafkaInputOperator<K>>>();
          for (Map.Entry<String, List<PartitionMetadata>> kp : kafkaPartitions.entrySet()) {
            String clusterId = kp.getKey();
            for (PartitionMetadata pm : kp.getValue()) {
              logger.info("[ONE_TO_ONE]: Create operator partition for cluster {}, topic {}, kafka partition {} ", clusterId, getConsumer().topic, pm.partitionId());
              newPartitions.add(createPartition(Sets.newHashSet(new KafkaPartition(clusterId, consumer.topic, pm.partitionId())), initOffset));
            }
          }
          resultPartitions = newPartitions;
          numPartitionsChanged = true;
        } else if (newWaitingPartition.size() != 0) {
          // add partition for new kafka partition
          for (KafkaPartition newPartition : newWaitingPartition) {
            logger.info("[ONE_TO_ONE]: Add operator partition for cluster {}, topic {}, partition {}", newPartition.getClusterId(), getConsumer().topic, newPartition.getPartitionId());
            partitions.add(createPartition(Sets.newHashSet(newPartition), null));
          }
          newWaitingPartition.clear();
          resultPartitions = partitions;
          numPartitionsChanged = true;
        }
        break;
      // For the 1 to N mapping The initial partition number is defined by stream application
      // Afterwards, the framework will dynamically adjust the partition and allocate consumers to as less operator partitions as it can
      //  and guarantee the total intake rate for each operator partition is below some threshold
      case ONE_TO_MANY:

        if (getConsumer() instanceof HighlevelKafkaConsumer) {
          throw new UnsupportedOperationException("[ONE_TO_MANY]: The high-level consumer is not supported for ONE_TO_MANY partition strategy.");
        }

        if (isInitialParitition || newWaitingPartition.size() != 0) {
          lastRepartitionTime = System.currentTimeMillis();
          logger.info("[ONE_TO_MANY]: Initializing partition(s)");
          // get partition metadata for topics.
          // Whatever operator is using high-level or simple kafka consumer, the operator always create a temporary simple kafka consumer to get the metadata of the topic
          // The initial value of brokerList of the KafkaConsumer is used to retrieve the topic metadata
          Map<String, List<PartitionMetadata>> kafkaPartitions = KafkaMetadataUtil.getPartitionsForTopic(getConsumer().brokers, getConsumer().getTopic());

          int size = initialPartitionCount;
          @SuppressWarnings("unchecked")
          Set<KafkaPartition>[] kps = (Set<KafkaPartition>[])Array.newInstance((new HashSet<KafkaPartition>()).getClass(), size);
          int i = 0;
          for (Map.Entry<String, List<PartitionMetadata>> en : kafkaPartitions.entrySet()) {
            String clusterId = en.getKey();
            for (PartitionMetadata pm : en.getValue()) {
              if (kps[i % size] == null) {
                kps[i % size] = new HashSet<KafkaPartition>();
              }
              kps[i % size].add(new KafkaPartition(clusterId, consumer.topic, pm.partitionId()));
              i++;
            }
          }
          size = i > size ? size : i;
          newPartitions = new ArrayList<Partitioner.Partition<AbstractKafkaInputOperator<K>>>(size);
          for (i = 0; i < size; i++) {
            logger.info("[ONE_TO_MANY]: Create operator partition for kafka partition(s): {} ", StringUtils.join(kps[i], ", "));
            newPartitions.add(createPartition(kps[i], initOffset));
          }
          // Add the existing partition Ids to the deleted operators
          for (Partition<AbstractKafkaInputOperator<K>> op : partitions) {
            deletedOperators.add(op.getPartitionedInstance().operatorId);
          }
          newWaitingPartition.clear();
          resultPartitions = newPartitions;
          numPartitionsChanged = true;
        }
        break;

      case ONE_TO_MANY_HEURISTIC:
        throw new UnsupportedOperationException("[ONE_TO_MANY_HEURISTIC]: Not implemented yet");
      default:
        break;
    }

    if (numPartitionsChanged) {
      List<WindowDataManager> managers = windowDataManager.partition(resultPartitions.size(), deletedOperators);
      int i = 0;
      for (Partition<AbstractKafkaInputOperator<K>> partition : resultPartitions) {
        partition.getPartitionedInstance().setWindowDataManager(managers.get(i++));
      }
    }
    return resultPartitions;
  }

  /**
   * Create a new partition with the partition Ids and initial offset positions
   *
   * @deprecated use {@link #createPartition(Set, Map)}
   */
  @Deprecated
  protected Partitioner.Partition<AbstractKafkaInputOperator<K>> createPartition(Set<KafkaPartition> pIds,
      Map<KafkaPartition, Long> initOffsets,
      @SuppressWarnings("UnusedParameters") Collection<WindowDataManager> newManagers)
  {
    return createPartition(pIds, initOffsets);
  }

  // Create a new partition with the partition Ids and initial offset positions
  protected Partitioner.Partition<AbstractKafkaInputOperator<K>> createPartition(Set<KafkaPartition> pIds,
      Map<KafkaPartition, Long> initOffsets)
  {

    Partitioner.Partition<AbstractKafkaInputOperator<K>> p = new DefaultPartition<>(KryoCloneUtils.cloneObject(this));
    if (p.getPartitionedInstance().getConsumer() instanceof SimpleKafkaConsumer) {
      p.getPartitionedInstance().getConsumer().resetPartitionsAndOffset(pIds, initOffsets);
      if (initOffsets != null) {
        //Don't send all offsets to all partitions
        //p.getPartitionedInstance().offsetStats.putAll(initOffsets);
        p.getPartitionedInstance().offsetStats.putAll(p.getPartitionedInstance().getConsumer().getCurrentOffsets());
      }
    }

    PartitionInfo pif = new PartitionInfo();
    pif.kpids = pIds;
    currentPartitionInfo.add(pif);
    return p;
  }

  @Override
  public StatsListener.Response processStats(StatsListener.BatchedOperatorStats stats)
  {
    StatsListener.Response resp = new StatsListener.Response();
    List<KafkaConsumer.KafkaMeterStats> kstats = extractKafkaStats(stats);
    resp.repartitionRequired = isPartitionRequired(stats.getOperatorId(), kstats);
    return resp;
  }

  private void updateOffsets(List<KafkaConsumer.KafkaMeterStats> kstats)
  {
    //In every partition check interval, call offsetmanager to update the offsets
    if (offsetManager != null) {
      Map<KafkaPartition, Long> offsetsForPartitions = getOffsetsForPartitions(kstats);
      if (offsetsForPartitions.size() > 0) {
        logger.debug("Passing offset updates to offset manager");
        offsetManager.updateOffsets(offsetsForPartitions);
      }
    }
  }

  private List<KafkaConsumer.KafkaMeterStats> extractKafkaStats(StatsListener.BatchedOperatorStats stats)
  {
    //preprocess the stats
    List<KafkaConsumer.KafkaMeterStats> kmsList = new LinkedList<KafkaConsumer.KafkaMeterStats>();
    for (Stats.OperatorStats os : stats.getLastWindowedStats()) {
      if (os != null && os.counters instanceof KafkaConsumer.KafkaMeterStats) {
        kmsList.add((KafkaConsumer.KafkaMeterStats)os.counters);
      }
    }
    return kmsList;
  }

  /**
   *
   * Check whether the operator needs repartition based on reported stats
   *
   * @return true if repartition is required
   * false if repartition is not required
   */
  private boolean isPartitionRequired(int opid, List<KafkaConsumer.KafkaMeterStats> kstats)
  {

    long t = System.currentTimeMillis();

    // If stats are available then update offsets
    // Do this before re-partition interval check below to not miss offset updates
    if (kstats.size() > 0) {
      logger.debug("Checking offset updates for offset manager");
      updateOffsets(kstats);
    }

    if (t - lastCheckTime < repartitionCheckInterval) {
      // return false if it's within repartitionCheckInterval since last time it check the stats
      return false;
    }

    if (repartitionInterval < 0) {
      // if repartition is disabled
      return false;
    }

    if (t - lastRepartitionTime < repartitionInterval) {
      // return false if it's still within repartitionInterval since last (re)partition
      return false;
    }


    kafkaStatsHolder.put(opid, kstats);

    if (kafkaStatsHolder.size() != currentPartitionInfo.size() || currentPartitionInfo.size() == 0) {
      // skip checking if the operator hasn't collected all the stats from all the current partitions
      return false;
    }

    try {

      // monitor if new kafka partition added
      {
        Set<KafkaPartition> existingIds = new HashSet<KafkaPartition>();
        for (PartitionInfo pio : currentPartitionInfo) {
          existingIds.addAll(pio.kpids);
        }

        Map<String, List<PartitionMetadata>> partitionsMeta = KafkaMetadataUtil.getPartitionsForTopic(consumer.brokers, consumer.getTopic());
        if (partitionsMeta == null) {
          //broker(s) has temporary issue to get metadata
          return false;
        }
        for (Map.Entry<String, List<PartitionMetadata>> en : partitionsMeta.entrySet()) {
          if (en.getValue() == null) {
            //broker(s) has temporary issue to get metadata
            continue;
          }
          for (PartitionMetadata pm : en.getValue()) {
            KafkaPartition pa = new KafkaPartition(en.getKey(), consumer.topic, pm.partitionId());
            if (!existingIds.contains(pa)) {
              newWaitingPartition.add(pa);
            }
          }
        }
        if (newWaitingPartition.size() != 0) {
          // found new kafka partition
          lastRepartitionTime = t;
          return true;
        }
      }

      return false;
    } finally {
      // update last  check time
      lastCheckTime = System.currentTimeMillis();
    }
  }

  public static enum PartitionStrategy
  {
    /**
     * Each operator partition connect to only one kafka partition
     */
    ONE_TO_ONE,
    /**
     * Each operator consumes from several kafka partitions with overall input rate under some certain hard limit in msgs/s or bytes/s
     * For now it <b>only</b> support <b>simple kafka consumer</b>
     */
    ONE_TO_MANY,
    /**
     * 1 to N partition based on the heuristic function
     * <b>NOT</b> implemented yet
     * TODO implement this later
     */
    ONE_TO_MANY_HEURISTIC
  }

  static class PartitionInfo
  {
    Set<KafkaPartition> kpids;
    long msgRateLeft;
    long byteRateLeft;
  }

  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  public void setInitialPartitionCount(int partitionCount)
  {
    this.initialPartitionCount = partitionCount;
  }

  public int getInitialPartitionCount()
  {
    return initialPartitionCount;
  }

  public long getMsgRateUpperBound()
  {
    return msgRateUpperBound;
  }

  public void setMsgRateUpperBound(long msgRateUpperBound)
  {
    this.msgRateUpperBound = msgRateUpperBound;
  }

  public long getByteRateUpperBound()
  {
    return byteRateUpperBound;
  }

  public void setByteRateUpperBound(long byteRateUpperBound)
  {
    this.byteRateUpperBound = byteRateUpperBound;
  }

  public void setInitialOffset(String initialOffset)
  {
    this.consumer.initialOffset = initialOffset;
  }

  public void setOffsetManager(OffsetManager offsetManager)
  {
    this.offsetManager = offsetManager;
  }

  public OffsetManager getOffsetManager()
  {
    return offsetManager;
  }

  public void setRepartitionCheckInterval(long repartitionCheckInterval)
  {
    this.repartitionCheckInterval = repartitionCheckInterval;
  }

  public long getRepartitionCheckInterval()
  {
    return repartitionCheckInterval;
  }

  public void setRepartitionInterval(long repartitionInterval)
  {
    this.repartitionInterval = repartitionInterval;
  }

  public long getRepartitionInterval()
  {
    return repartitionInterval;
  }

  //@Pattern(regexp="ONE_TO_ONE|ONE_TO_MANY|ONE_TO_MANY_HEURISTIC", flags={Flag.CASE_INSENSITIVE})
  public void setStrategy(String policy)
  {
    this.strategy = PartitionStrategy.valueOf(policy.toUpperCase());
  }
}
