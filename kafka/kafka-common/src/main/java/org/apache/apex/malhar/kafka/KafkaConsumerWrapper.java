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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * This is the wrapper class for new Kafka consumer API
 *
 * It starts number of consumers(one for each cluster) in same number of threads.
 * Maintains the consumer offsets
 *
 * It also use the consumers to commit the application processed offsets along with the application name
 *
 *
 * @since 3.3.0
 */
@InterfaceStability.Evolving
public class KafkaConsumerWrapper implements Closeable
{

  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerWrapper.class);

  private AtomicBoolean isAlive = new AtomicBoolean(false);

  private final Map<String, AbstractKafkaConsumer> consumers = new HashMap<>();

  // The in memory buffer hold consumed messages
  private ArrayBlockingQueue<Pair<String, ConsumerRecord<byte[], byte[]>>> holdingBuffer;

  private AbstractKafkaInputOperator ownerOperator = null;

  private ExecutorService kafkaConsumerExecutor;

  private final Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetsToCommit = new HashMap<>();

  private boolean waitForReplay = false;

  private final List<Future<?>> kafkaConsumerThreads = new ArrayList<>();

  public boolean hasAnyKafkaReaderThreadDied()
  {
    for (Future<?> future : kafkaConsumerThreads) {
      if (future.isDone() || future.isCancelled()) {
        return true;
      }
    }

    return false;
  }

  /**
   *
   * Only put the offset needs to be committed in the ConsumerThread.offsetToCommit map
   * The consumer thread will commit the offset(s)
   *
   * @param offsetsInWindow
   */
  public void commitOffsets(Map<AbstractKafkaPartitioner.PartitionMeta, Long> offsetsInWindow)
  {
    if (offsetsInWindow == null) {
      return;
    }

    // group offsets by cluster and topic partition
    for (Map.Entry<AbstractKafkaPartitioner.PartitionMeta, Long> e : offsetsInWindow.entrySet()) {
      String cluster = e.getKey().getCluster();
      Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetMap = offsetsToCommit.get(cluster);
      if (topicPartitionOffsetMap == null) {
        logger.warn("committed offset map should be initialized by consumer thread!");
        continue;
      }
      topicPartitionOffsetMap.put(e.getKey().getTopicPartition(), new OffsetAndMetadata(e.getValue()));
    }

  }

  public void emitImmediately(Map<AbstractKafkaPartitioner.PartitionMeta, Pair<Long, Long>> windowData)
  {
    for (Map.Entry<AbstractKafkaPartitioner.PartitionMeta, Pair<Long, Long>> windowEntry : windowData.entrySet()) {
      AbstractKafkaPartitioner.PartitionMeta meta = windowEntry.getKey();
      Pair<Long, Long> replayOffsetSize = windowEntry.getValue();
      AbstractKafkaConsumer kc = consumers.get(meta.getCluster());
      if (kc == null && kc.isConsumerContainsPartition(windowEntry.getKey().getTopicPartition())) {
        throw new RuntimeException("Coundn't find consumer to replay the message PartitionMeta : " + meta);
      }
      //pause other partition
      for (TopicPartition tp : kc.getPartitions()) {
        if (meta.getTopicPartition().equals(tp)) {
          kc.resumePartition(tp);
        } else {
          try {
            kc.positionPartition(tp);
          } catch (NoOffsetForPartitionException e) {
            //the poll() method of a consumer will throw exception
            // if any of subscribed consumers not initialized with position
            handleNoOffsetForPartitionException(e, kc);
          }
          kc.pausePartition(tp);
        }
      }
      // set the offset to window start offset
      kc.seekToOffset(meta.getTopicPartition(), replayOffsetSize.getLeft());
      long windowCount = replayOffsetSize.getRight();
      while (windowCount > 0) {
        try {
          ConsumerRecords<byte[], byte[]> records = kc.pollRecords(ownerOperator.getConsumerTimeout());
          for (Iterator<ConsumerRecord<byte[], byte[]>> cri = records.iterator(); cri.hasNext() && windowCount > 0; ) {
            ownerOperator.emitTuple(meta.getCluster(), cri.next());
            windowCount--;
          }
        } catch (NoOffsetForPartitionException e) {
          throw new RuntimeException("Couldn't replay the offset", e);
        }
      }
      // set the offset after window
      kc.seekToOffset(meta.getTopicPartition(), replayOffsetSize.getLeft() + replayOffsetSize.getRight());
    }

    // resume all topics
    for (AbstractKafkaConsumer kc : consumers.values()) {
      kc.resumeAllPartitions();
    }

  }

  public void afterReplay()
  {
    waitForReplay = false;
  }

  static final class ConsumerThread implements Runnable
  {

    private final AbstractKafkaConsumer consumer;

    private final String cluster;

    private final KafkaConsumerWrapper wrapper;

    private Map<TopicPartition, OffsetAndMetadata> offsetToCommit = null;

    public ConsumerThread(String cluster, AbstractKafkaConsumer consumer, KafkaConsumerWrapper wrapper)
    {
      this.cluster = cluster;
      this.consumer = consumer;
      this.wrapper = wrapper;
      this.offsetToCommit = new ConcurrentHashMap<>();
      wrapper.offsetsToCommit.put(cluster, offsetToCommit);
    }

    @Override
    public void run()
    {
      try {

        while (wrapper.isAlive.get()) {
          if (wrapper.waitForReplay) {
            Thread.sleep(100);
            continue;
          }
          if (!this.offsetToCommit.isEmpty()) {
            // in each fetch cycle commit the offset if needed
            if (logger.isDebugEnabled()) {
              logger.debug("Commit offsets {}", Joiner.on(';').withKeyValueSeparator("=").join(this.offsetToCommit));
            }
            consumer.commitAsync(offsetToCommit, wrapper.ownerOperator);
            offsetToCommit.clear();
          }
          try {
            ConsumerRecords<byte[], byte[]> records = consumer.pollRecords(wrapper.ownerOperator.getConsumerTimeout());
            for (ConsumerRecord<byte[], byte[]> record : records) {
              wrapper.putMessage(Pair.of(cluster, record));
            }
          } catch (NoOffsetForPartitionException e) {
            wrapper.handleNoOffsetForPartitionException(e, consumer);
          } catch (InterruptedException e) {
            throw new IllegalStateException("Consumer thread is interrupted unexpectedly", e);
          }
        }
      } catch (WakeupException we) {
        logger.error("The consumer is being stopped");
      } catch (InterruptedException e) {
        logger.error("Kafka consumer thread was interrupted. {}", e);
        DTThrowable.rethrow(e);
      } catch (Throwable ex) {
        logger.error("Kafka consumer wrapper thread failed with the exception {}", ex);
      } finally {
        logger.error("Exiting Kafka consumer thread.");
        consumer.close();
      }
    }
  }

  protected void handleNoOffsetForPartitionException(NoOffsetForPartitionException e,
      AbstractKafkaConsumer consumer)
  {
    // if initialOffset is set to EARLIST or LATEST
    // and the application is run as first time
    // then there is no existing committed offset and this error will be caught
    // we need to seek to either beginning or end of the partition
    // based on the initial offset setting
    AbstractKafkaInputOperator.InitialOffset io =
        AbstractKafkaInputOperator.InitialOffset.valueOf(ownerOperator.getInitialOffset());
    if (io == AbstractKafkaInputOperator.InitialOffset.APPLICATION_OR_EARLIEST
        || io == AbstractKafkaInputOperator.InitialOffset.EARLIEST) {
      consumer.seekToBeginning(e.partitions().toArray(new TopicPartition[0]));
    } else {
      consumer.seekToEnd(e.partitions().toArray(new TopicPartition[0]));
    }

  }

  /**
   * This method is called in setup method of Abstract Kafka Input Operator
   */
  public void create(AbstractKafkaInputOperator ownerOperator)
  {
    holdingBuffer = new ArrayBlockingQueue<>(ownerOperator.getHoldingBufferSize());
    this.ownerOperator = ownerOperator;
    logger.info("Create consumer wrapper with holding buffer size: {} ", ownerOperator.getHoldingBufferSize());
    if (logger.isInfoEnabled()) {
      logger.info("Assignments are {} ", Joiner.on('\n').join(ownerOperator.assignment()));
    }
  }

  /**
   * This method is called in the activate method of the operator
   */
  public void start(boolean waitForReplay)
  {
    this.waitForReplay = waitForReplay;
    isAlive.set(true);

    // thread to consume the kafka data
    // create thread pool for consumer threads
    kafkaConsumerExecutor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setNameFormat("kafka-consumer-%d").build());

    // group list of PartitionMeta by cluster
    Map<String, List<TopicPartition>> consumerAssignment = new HashMap<>();
    Set<AbstractKafkaPartitioner.PartitionMeta> assignments = ownerOperator.assignment();
    for (AbstractKafkaPartitioner.PartitionMeta partitionMeta : assignments) {
      String cluster = partitionMeta.getCluster();
      List<TopicPartition> cAssignment = consumerAssignment.get(cluster);
      if (cAssignment == null) {
        cAssignment = new LinkedList<>();
        consumerAssignment.put(cluster, cAssignment);
      }
      cAssignment.add(new TopicPartition(partitionMeta.getTopic(), partitionMeta.getPartitionId()));
    }

    Map<AbstractKafkaPartitioner.PartitionMeta, Long> currentOffset = ownerOperator.getOffsetTrack();

    //  create one thread for each cluster
    // each thread use one KafkaConsumer to consume from 1+ partition(s) of 1+ topic(s)
    for (Map.Entry<String, List<TopicPartition>> e : consumerAssignment.entrySet()) {

      Properties prop = new Properties();
      if (ownerOperator.getConsumerProps() != null) {
        prop.putAll(ownerOperator.getConsumerProps());
      }

      prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, e.getKey());
      prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
      // never auto commit the offsets
      prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      AbstractKafkaInputOperator.InitialOffset initialOffset =
          AbstractKafkaInputOperator.InitialOffset.valueOf(ownerOperator.getInitialOffset());

      if (initialOffset == AbstractKafkaInputOperator.InitialOffset.APPLICATION_OR_EARLIEST ||
          initialOffset == AbstractKafkaInputOperator.InitialOffset.APPLICATION_OR_LATEST) {
        // commit the offset with application name if we set initialoffset to application
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, ownerOperator.getApplicationName() + "_Consumer");
      }

      AbstractKafkaConsumer kc = ownerOperator.createConsumer(prop);
      kc.assignPartitions(e.getValue());
      if (logger.isInfoEnabled()) {
        logger.info("Create consumer with properties {} ", Joiner.on(";").withKeyValueSeparator("=").join(prop));
        logger.info("Assign consumer to {}", Joiner.on('#').join(e.getValue()));
      }
      if (currentOffset != null && !currentOffset.isEmpty()) {
        for (TopicPartition tp : e.getValue()) {
          AbstractKafkaPartitioner.PartitionMeta partitionKey =
              new AbstractKafkaPartitioner.PartitionMeta(e.getKey(), tp.topic(), tp.partition());
          if (currentOffset.containsKey(partitionKey)) {
            kc.seekToOffset(tp, currentOffset.get(partitionKey));
          }
        }
      }

      consumers.put(e.getKey(), kc);
      Future<?> future = kafkaConsumerExecutor.submit(new ConsumerThread(e.getKey(), kc, this));
      kafkaConsumerThreads.add(future);
    }
  }

  /**
   * The method is called in the deactivate method of the operator
   */
  public void stop()
  {
    isAlive.set(false);
    for (AbstractKafkaConsumer c : consumers.values()) {
      c.wakeup();
    }
    kafkaConsumerExecutor.shutdownNow();
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

  public Pair<String, ConsumerRecord<byte[], byte[]>> pollMessage()
  {
    return holdingBuffer.poll();
  }

  public int messageSize()
  {
    return holdingBuffer.size();
  }

  protected final void putMessage(Pair<String, ConsumerRecord<byte[], byte[]>> msg) throws InterruptedException
  {
    // block from receiving more message
    holdingBuffer.put(msg);
  }

  @Override
  public void close() throws IOException
  {
  }

  public Map<String, Map<MetricName, ? extends Metric>> getAllConsumerMetrics()
  {
    Map<String, Map<MetricName, ? extends Metric>> val = new HashMap<>();
    for (Map.Entry<String, AbstractKafkaConsumer> e : consumers.entrySet()) {
      val.put(e.getKey(), e.getValue().metrics());
    }
    return val;
  }
}
