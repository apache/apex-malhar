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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * The abstract kafka input operator using kafka 0.9.0 new consumer API
 * A scalable, fault-tolerant, at-least-once kafka input operator
 * Key features includes:
 *
 * <ol>
 * <li>Out-of-box One-to-one and one-to-many partition strategy support plus customizable partition strategy
 * refer to AbstractKafkaPartitioner </li>
 * <li>Fault-tolerant when the input operator goes down, it redeploys on other node</li>
 * <li>At-least-once semantics for operator failure (no matter which operator fails)</li>
 * <li>At-least-once semantics for cold restart (no data loss even if you restart the application)</li>
 * <li>Multi-cluster support, one operator can consume data from more than one kafka clusters</li>
 * <li>Multi-topic support, one operator can subscribe multiple topics</li>
 * <li>Throughput control support, you can throttle number of tuple for each streaming window</li>
 * </ol>
 *
 * @since 3.3.0
 */
@InterfaceStability.Evolving
public abstract class AbstractKafkaInputOperator implements InputOperator,
    Operator.ActivationListener<Context.OperatorContext>, Operator.CheckpointNotificationListener,
    Partitioner<AbstractKafkaInputOperator>, StatsListener, OffsetCommitCallback
{

  private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaInputOperator.class);

  static {
    // We create new consumers periodically to pull metadata (Kafka consumer keeps metadata in cache)
    // Skip log4j log for ConsumerConfig class to avoid too much noise in application
    LogManager.getLogger(ConsumerConfig.class).setLevel(Level.WARN);
  }

  public enum InitialOffset
  {
    EARLIEST, // consume from beginning of the partition every time when application restart
    LATEST, // consume from latest of the partition every time when application restart
    // consume from committed position from last run or earliest if there is no committed offset(s)
    APPLICATION_OR_EARLIEST,
    APPLICATION_OR_LATEST // consume from committed position from last run or latest if there is no committed offset(s)
  }

  @NotNull
  private String[] clusters;

  @NotNull
  private String[] topics;

  /**
   * offset track for checkpoint
   */
  private final Map<AbstractKafkaPartitioner.PartitionMeta, Long> offsetTrack = new HashMap<>();

  private final transient Map<AbstractKafkaPartitioner.PartitionMeta, Long> windowStartOffset = new HashMap<>();

  private transient int operatorId;

  private int initialPartitionCount = 1;

  private long repartitionInterval = 30000L;

  private long repartitionCheckInterval = 5000L;

  @Min(1)
  private int maxTuplesPerWindow = Integer.MAX_VALUE;

  /**
   * By default the operator start consuming from the committed offset or the latest one
   */
  private InitialOffset initialOffset = InitialOffset.APPLICATION_OR_LATEST;

  private long metricsRefreshInterval = 5000L;

  private long consumerTimeout = 5000L;

  private int holdingBufferSize = 1024;

  private Properties consumerProps = new Properties();

  /**
   * Assignment for each operator instance
   */
  private Set<AbstractKafkaPartitioner.PartitionMeta> assignment;

  //=======================All transient fields==========================

  /**
   * Wrapper consumer object
   * It wraps KafkaConsumer, maintains consumer thread and store messages in a queue
   */
  private final transient KafkaConsumerWrapper consumerWrapper = createConsumerWrapper();

  /**
   * By default the strategy is one to one
   *
   * @see PartitionStrategy
   */
  private PartitionStrategy strategy = PartitionStrategy.ONE_TO_ONE;

  /**
   * count the emitted message in each window<br>
   * non settable
   */
  private transient int emitCount = 0;

  /**
   * store offsets with window id, only keep offsets with windows that have not been committed
   */
  private final transient List<Pair<Long, Map<AbstractKafkaPartitioner.PartitionMeta, Long>>> offsetHistory =
      new LinkedList<>();

  /**
   * Application name is used as group.id for kafka consumer
   */
  private transient String applicationName;

  private transient AbstractKafkaPartitioner partitioner;

  private transient long currentWindowId;

  private transient long lastCheckTime = 0L;

  private transient long lastRepartitionTime = 0L;

  @AutoMetric
  private transient KafkaMetrics metrics;

  private WindowDataManager windowDataManager = new WindowDataManager.NoopWindowDataManager();

  private transient volatile Throwable consumerError;

  /**
   * Creates the Wrapper consumer object
   * It maintains consumer thread and store messages in a queue
   * @return KafkaConsumerWrapper
   */
  public KafkaConsumerWrapper createConsumerWrapper()
  {
    return new KafkaConsumerWrapper();
  }

  // Creates the consumer object and it wraps KafkaConsumer.
  public abstract AbstractKafkaConsumer createConsumer(Properties prop);

  @Override
  public void activate(Context.OperatorContext context)
  {
    consumerWrapper.start(isIdempotent());
  }

  @Override
  public void deactivate()
  {
    consumerWrapper.stop();
  }

  @Override
  public void checkpointed(long l)
  {

  }

  @Override
  public void beforeCheckpoint(long windowId)
  {

  }

  @Override
  public void committed(long windowId)
  {
    if (initialOffset == InitialOffset.LATEST || initialOffset == InitialOffset.EARLIEST) {
      return;
    }
    //ask kafka consumer wrapper to store the committed offsets
    for (Iterator<Pair<Long, Map<AbstractKafkaPartitioner.PartitionMeta, Long>>> iter =
        offsetHistory.iterator(); iter.hasNext(); ) {
      Pair<Long, Map<AbstractKafkaPartitioner.PartitionMeta, Long>> item = iter.next();
      if (item.getLeft() <= windowId) {
        if (item.getLeft() == windowId) {
          consumerWrapper.commitOffsets(item.getRight());
        }
        iter.remove();
      }
    }
    if (isIdempotent()) {
      try {
        windowDataManager.committed(windowId);
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void emitTuples()
  {
    int count = consumerWrapper.messageSize();
    if (maxTuplesPerWindow > 0) {
      count = Math.min(count, maxTuplesPerWindow - emitCount);
    }
    for (int i = 0; i < count; i++) {
      Pair<String, ConsumerRecord<byte[], byte[]>> tuple = consumerWrapper.pollMessage();
      ConsumerRecord<byte[], byte[]> msg = tuple.getRight();
      emitTuple(tuple.getLeft(), msg);
      AbstractKafkaPartitioner.PartitionMeta pm = new AbstractKafkaPartitioner.PartitionMeta(tuple.getLeft(),
          msg.topic(), msg.partition());
      offsetTrack.put(pm, msg.offset() + 1);
      if (isIdempotent() && !windowStartOffset.containsKey(pm)) {
        windowStartOffset.put(pm, msg.offset());
      }
    }
    emitCount += count;
    processConsumerError();
  }

  protected abstract void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message);

  @Override
  public void beginWindow(long wid)
  {
    emitCount = 0;
    currentWindowId = wid;
    windowStartOffset.clear();
    if (isIdempotent() && wid <= windowDataManager.getLargestCompletedWindow()) {
      replay(wid);
    } else {
      consumerWrapper.afterReplay();
    }
  }

  private void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      Map<AbstractKafkaPartitioner.PartitionMeta, Pair<Long, Long>> windowData =
          (Map<AbstractKafkaPartitioner.PartitionMeta, Pair<Long, Long>>)windowDataManager.retrieve(windowId);
      consumerWrapper.emitImmediately(windowData);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void endWindow()
  {
    // copy current offset track to history memory
    Map<AbstractKafkaPartitioner.PartitionMeta, Long> offsetsWithWindow = new HashMap<>(offsetTrack);
    offsetHistory.add(Pair.of(currentWindowId, offsetsWithWindow));

    //update metrics
    metrics.updateMetrics(clusters, consumerWrapper.getAllConsumerMetrics());

    //update the windowDataManager
    if (isIdempotent()) {
      try {
        Map<AbstractKafkaPartitioner.PartitionMeta, Pair<Long, Long>> windowData = new HashMap<>();
        for (Map.Entry<AbstractKafkaPartitioner.PartitionMeta, Long> e : windowStartOffset.entrySet()) {
          windowData.put(e.getKey(), new MutablePair<>(e.getValue(), offsetTrack.get(e.getKey()) - e.getValue()));
        }
        windowDataManager.save(windowData, currentWindowId);
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    applicationName = context.getValue(Context.DAGContext.APPLICATION_NAME);
    consumerWrapper.create(this);
    metrics = new KafkaMetrics(metricsRefreshInterval);
    windowDataManager.setup(context);
    operatorId = context.getId();
  }

  @Override
  public void teardown()
  {
    windowDataManager.teardown();
  }

  protected void processConsumerError()
  {
    if (consumerError != null) {
      logger.error("Error in consumer, terminating");
      throw Throwables.propagate(consumerError);
    }
  }

  private void initPartitioner()
  {
    if (partitioner == null) {
      logger.info("Initialize Partitioner");
      switch (strategy) {
        case ONE_TO_ONE:
          partitioner = new OneToOnePartitioner(clusters, topics, this);
          break;
        case ONE_TO_MANY:
          partitioner = new OneToManyPartitioner(clusters, topics, this);
          break;
        case ONE_TO_MANY_HEURISTIC:
          throw new UnsupportedOperationException("Not implemented yet");
        default:
          throw new RuntimeException("Invalid strategy");
      }
      logger.info("Actual Partitioner is {}", partitioner.getClass());
    }

  }

  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    long t = System.currentTimeMillis();
    if (repartitionInterval < 0 || repartitionCheckInterval < 0 ||
        t - lastCheckTime < repartitionCheckInterval || t - lastRepartitionTime < repartitionInterval) {
      // return false if it's within repartitionCheckInterval since last time it check the stats
      Response response = new Response();
      response.repartitionRequired = false;
      return response;
    }

    try {
      logger.debug("Process stats");
      initPartitioner();
      return partitioner.processStats(batchedOperatorStats);
    } finally {
      lastCheckTime = System.currentTimeMillis();
    }
  }

  @Override
  public Collection<Partition<AbstractKafkaInputOperator>> definePartitions(
      Collection<Partition<AbstractKafkaInputOperator>> collection, PartitioningContext partitioningContext)
  {
    logger.debug("Define partitions");
    initPartitioner();
    return partitioner.definePartitions(collection, partitioningContext);
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractKafkaInputOperator>> map)
  {
    // update the last repartition time
    lastRepartitionTime = System.currentTimeMillis();
    initPartitioner();
    partitioner.partitioned(map);
  }

  /**
   * A callback from consumer after it commits the offset
   *
   * @param map
   * @param e
   */
  @Override
  public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Commit offsets complete {} ", Joiner.on(';').withKeyValueSeparator("=").join(map));
    }
    if (e != null) {
      consumerError = e;
      logger.warn("Exceptions in committing offsets {} : {} ",
          Joiner.on(';').withKeyValueSeparator("=").join(map), e);
    }
  }

  public void assign(Set<AbstractKafkaPartitioner.PartitionMeta> assignment)
  {
    this.assignment = assignment;
  }

  public Set<AbstractKafkaPartitioner.PartitionMeta> assignment()
  {
    return assignment;
  }

  private boolean isIdempotent()
  {
    return windowDataManager != null && !(windowDataManager instanceof WindowDataManager.NoopWindowDataManager);
  }

  //---------------------------------------------setters and getters----------------------------------------
  public void setInitialPartitionCount(int partitionCount)
  {
    this.initialPartitionCount = partitionCount;
  }

  /**
   * initial partition count
   * only used with PartitionStrategy.ONE_TO_MANY
   * or customized strategy
   */
  public int getInitialPartitionCount()
  {
    return initialPartitionCount;
  }

  public void setClusters(String clusters)
  {
    this.clusters = clusters.split(";");
  }

  /**
   * Same setting as bootstrap.servers property to KafkaConsumer
   * refer to http://kafka.apache.org/documentation.html#newconsumerconfigs
   * To support multi cluster, you can have multiple bootstrap.servers separated by ";"
   */
  public String getClusters()
  {
    return Joiner.on(';').join(clusters);
  }

  public void setTopics(String topics)
  {
    this.topics = Iterables.toArray(Splitter.on(',').trimResults().omitEmptyStrings().split(topics), String.class);
  }

  /**
   * The topics the operator consumes, separate by','
   * Topic name can only contain ASCII alphanumerics, '.', '_' and '-'
   */
  public String getTopics()
  {
    return Joiner.on(", ").join(topics);
  }

  public void setStrategy(String policy)
  {
    this.strategy = PartitionStrategy.valueOf(policy.toUpperCase());
  }

  public String getStrategy()
  {
    return strategy.name();
  }

  public void setInitialOffset(String initialOffset)
  {
    this.initialOffset = InitialOffset.valueOf(initialOffset.toUpperCase());
  }

  /**
   * Initial offset, it should be one of the following
   * <ul>
   * <li>earliest</li>
   * <li>latest</li>
   * <li>application_or_earliest</li>
   * <li>application_or_latest</li>
   * </ul>
   */
  public String getInitialOffset()
  {
    return initialOffset.name();
  }

  public String getApplicationName()
  {
    return applicationName;
  }

  public void setConsumerProps(Properties consumerProps)
  {
    this.consumerProps = consumerProps;
  }

  /**
   * Extra kafka consumer properties
   * http://kafka.apache.org/090/documentation.html#newconsumerconfigs
   *
   * Please be aware that the properties below are set by the operator, don't override it
   *
   * <ul>
   * <li>bootstrap.servers</li>
   * <li>group.id</li>
   * <li>auto.offset.reset</li>
   * <li>enable.auto.commit</li>
   * <li>partition.assignment.strategy</li>
   * <li>key.deserializer</li>
   * <li>value.deserializer</li>
   * </ul>
   */
  public Properties getConsumerProps()
  {
    return consumerProps;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  /**
   * maximum tuples allowed to be emitted in each window
   */
  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  /**
   * @see <a href="http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#poll(long)">
   * org.apache.kafka.clients.consumer.KafkaConsumer.poll</a>
   */
  public long getConsumerTimeout()
  {
    return consumerTimeout;
  }

  public void setConsumerTimeout(long consumerTimeout)
  {
    this.consumerTimeout = consumerTimeout;
  }

  /**
   * Number of messages kept in memory waiting for emission to downstream operator
   */
  public int getHoldingBufferSize()
  {
    return holdingBufferSize;
  }

  public void setHoldingBufferSize(int holdingBufferSize)
  {
    this.holdingBufferSize = holdingBufferSize;
  }

  /**
   * metrics refresh interval
   */
  public long getMetricsRefreshInterval()
  {
    return metricsRefreshInterval;
  }

  public void setMetricsRefreshInterval(long metricsRefreshInterval)
  {
    this.metricsRefreshInterval = metricsRefreshInterval;
  }

  public void setRepartitionCheckInterval(long repartitionCheckInterval)
  {
    this.repartitionCheckInterval = repartitionCheckInterval;
  }

  /**
   * Minimal interval between checking collected stats and decide whether it needs to repartition or not.
   * And minimal interval between 2 offset updates
   */
  public long getRepartitionCheckInterval()
  {
    return repartitionCheckInterval;
  }

  public void setRepartitionInterval(long repartitionInterval)
  {
    this.repartitionInterval = repartitionInterval;
  }

  /**
   * Minimal interval between 2 (re)partition actions
   */
  public long getRepartitionInterval()
  {
    return repartitionInterval;
  }

  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  /**
   * @return current checkpointed offsets
   * @omitFromUI
   */
  public Map<AbstractKafkaPartitioner.PartitionMeta, Long> getOffsetTrack()
  {
    return offsetTrack;
  }
}
