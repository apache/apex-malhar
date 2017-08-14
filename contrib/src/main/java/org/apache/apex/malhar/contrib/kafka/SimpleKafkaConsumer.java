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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/**
 * Simple kafka consumer adaptor used by kafka input operator Properties:<br>
 * <b>timeout</b>: Timeout for connection and ping <br>
 * <b>bufferSize</b>: buffer size of the consumer <br>
 * <b>clientId</b>: client id of the consumer <br>
 * <b>partitionIds</b>: partition id that the consumer want to consume <br>
 * <li>(-1): create #partition threads and consumers to read the topic from different partitions in parallel</li> <br>
 * <b>metadataRefreshInterval</b>: The interval that the monitor thread use to monitor the broker leadership change <br>
 * <b>metadataRetrievalRetry</b>: Maximum retry times for metadata retrieval failures<br>
 * default value is 3 <br>
 * -1: unlimited retry <br>
 * <br>
 *
 * Load balance: <br>
 * <li>The consumer create several data-consuming threads to consume the data from broker(s)</li>
 * <li>Each thread has only ONE kafka client connecting to ONE broker to consume data from for multiple partitions </li>
 * <li>
 * There is ONE separate thread to monitor the leadership for all the partitions of the topic at every
 * #metadataRefreshInterval milliseconds</li>
 * <li>Once leadership
 * change detected(leader broker failure, or server-side reassignment), it switches to the new leader broker</li> <li>
 * For server-side leadership change, see kafka-preferred-replica-election.sh and kafka-reassign-partitions.sh</li> <br>
 * <br>
 * Kafka broker failover: <br>
 * <li>Once broker failure is detected, it waits #metadataRefreshInterval to reconnect to the new leader broker</li> <li>
 * If there are consecutive #metadataRetrievalRetry failures to retrieve the metadata for the topic. It will stop
 * consuming the partition</li> <br>
 *
 * @since 0.9.0
 */
public class SimpleKafkaConsumer extends KafkaConsumer
{

  /**
   * The data-consuming thread that use one simple kafka client to connect to one broker which is the leader of the partition(s) that this consumer is interested
   */
  static final class ConsumerThread implements Runnable
  {
    private final Broker broker;
    private final String clientName;
    // kafka simple consumer object
    private SimpleConsumer ksc;
    // The SimpleKafkaConsumer which holds this thread
    private SimpleKafkaConsumer consumer;
    // partitions consumed in this thread
    private final Set<KafkaPartition> kpS;
    @SuppressWarnings("rawtypes")
    private Future threadItSelf;

    private ConsumerThread(Broker broker, Set<KafkaPartition> kpl, SimpleKafkaConsumer consumer)
    {
      this.broker = broker;
      this.clientName = consumer.getClientName(broker.host() + "_" + broker.port());
      this.consumer = consumer;
      this.kpS = Collections.newSetFromMap(new ConcurrentHashMap<KafkaPartition, Boolean>());
      this.kpS.addAll(kpl);
    }

    @Override
    public void run()
    {

      try {
        logger.info("Connecting to broker {} [ timeout:{}, buffersize:{}, clientId: {}]", broker, consumer.timeout, consumer.bufferSize, clientName);
        ksc = new SimpleConsumer(broker.host(), broker.port(), consumer.timeout, consumer.bufferSize, clientName);
        // Initialize all start offsets for all kafka partitions read from this consumer
        // read either from beginning of the broker or last offset committed by the operator
        for (KafkaPartition kpForConsumer : kpS) {
          logger.info("Start consuming data of topic {} ", kpForConsumer);
          if (consumer.offsetTrack.get(kpForConsumer) != null) {
            // start from recovery
            // offsets.put(kpForConsumer, offsetTrack.get(kpForConsumer));
            logger.info("Partition {} initial offset {}", kpForConsumer, consumer.offsetTrack.get(kpForConsumer));
          } else {
            long startOffsetReq = consumer.initialOffset.equalsIgnoreCase("earliest") ? OffsetRequest.EarliestTime() : OffsetRequest.LatestTime();
            logger.info("Partition {} initial offset {} {}", kpForConsumer.getPartitionId(), startOffsetReq, consumer.initialOffset);
            consumer.offsetTrack.put(kpForConsumer, KafkaMetadataUtil.getLastOffset(ksc, consumer.topic, kpForConsumer.getPartitionId(), startOffsetReq, clientName));
          }
        }

        // stop consuming only when the consumer container is stopped or the metadata can not be refreshed
        while (consumer.isAlive && (consumer.metadataRefreshRetryLimit == -1 || consumer.retryCounter.get() < consumer.metadataRefreshRetryLimit)) {

          if (kpS == null || kpS.isEmpty()) {
            return;
          }

          FetchRequestBuilder frb = new FetchRequestBuilder().clientId(clientName);
          // add all partition request in one Fretch request together
          for (KafkaPartition kpForConsumer : kpS) {
            frb.addFetch(consumer.topic, kpForConsumer.getPartitionId(), consumer.offsetTrack.get(kpForConsumer), consumer.bufferSize);
          }

          FetchRequest req = frb.build();
          if (ksc == null) {
            if (consumer.metadataRefreshInterval > 0) {
              Thread.sleep(consumer.metadataRefreshInterval + 1000);
            } else {
              Thread.sleep(100);
            }
          }
          FetchResponse fetchResponse = ksc.fetch(req);
          for (Iterator<KafkaPartition> iterator = kpS.iterator(); iterator.hasNext();) {
            KafkaPartition kafkaPartition = iterator.next();
            short errorCode = fetchResponse.errorCode(consumer.topic, kafkaPartition.getPartitionId());
            if (fetchResponse.hasError() && errorCode != ErrorMapping.NoError()) {
              // Kick off partition(s) which has error when fetch from this broker temporarily
              // Monitor will find out which broker it goes in monitor thread
              logger.warn("Error when consuming topic {} from broker {} with error {} ", kafkaPartition, broker,
                  ErrorMapping.exceptionFor(errorCode));
              if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
                long seekTo = consumer.initialOffset.toLowerCase().equals("earliest") ? OffsetRequest.EarliestTime()
                    : OffsetRequest.LatestTime();
                seekTo = KafkaMetadataUtil.getLastOffset(ksc, consumer.topic, kafkaPartition.getPartitionId(), seekTo, clientName);
                logger.warn("Offset out of range error, reset offset to {}", seekTo);
                consumer.offsetTrack.put(kafkaPartition, seekTo);
                continue;
              }
              iterator.remove();
              consumer.partitionToBroker.remove(kafkaPartition);
              consumer.stats.updatePartitionStats(kafkaPartition, -1, "");
              continue;
            }
            // If the fetchResponse either has no error or the no error for $kafkaPartition get the data
            long offset = -1L;
            for (MessageAndOffset msg : fetchResponse.messageSet(consumer.topic, kafkaPartition.getPartitionId())) {
              offset = msg.nextOffset();
              consumer.putMessage(kafkaPartition, msg.message(), msg.offset());
            }
            if (offset != -1) {
              consumer.offsetTrack.put(kafkaPartition, offset);
            }
          }
        }
      } catch (Exception e) {
        logger.error("The consumer encounters an unrecoverable exception. Close the connection to broker {} \n Caused by {}", broker, e);
      } finally {
        if (ksc != null) {
          ksc.close();
        }
        for (KafkaPartition kpForConsumer : kpS) {
          // Update consumer that these partitions are currently stop being consumed because of some unrecoverable exception
          consumer.partitionToBroker.remove(kpForConsumer);
        }

        logger.info("Exit the consumer thread for broker {} ", broker);
      }
    }

    public void addPartitions(Set<KafkaPartition> newKps)
    {
      // Add the partition(s) to this existing consumer thread they are assigned to this broker
      kpS.addAll(newKps);

    }

    @SuppressWarnings("rawtypes")
    public Future getThreadItSelf()
    {
      return threadItSelf;
    }

    public void setThreadItSelf(@SuppressWarnings("rawtypes") Future threadItSelf)
    {
      this.threadItSelf = threadItSelf;
    }
  }

  public SimpleKafkaConsumer()
  {
    super();
  }

  public SimpleKafkaConsumer(String topic, int timeout, int bufferSize, String clientId)
  {
    this(topic, timeout, bufferSize, clientId, null);
  }

  public SimpleKafkaConsumer(String topic, int timeout, int bufferSize, String clientId, Set<KafkaPartition> partitionIds)
  {
    super(topic);
    this.timeout = timeout;
    this.bufferSize = bufferSize;
    this.clientId = clientId;
    this.kps = partitionIds;
  }

  public SimpleKafkaConsumer(String zks, String topic, int timeout, int bufferSize, String clientId, Set<KafkaPartition> partitionIds)
  {
    super(zks, topic);
    this.timeout = timeout;
    this.bufferSize = bufferSize;
    this.clientId = clientId;
    this.kps = partitionIds;
  }

  private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

  /**
   * Track consumers connected to each broker, topics and partitions hosted on same broker are consumed by same
   * consumer. Clean the resource if necessary. Key is the kafka broker object.
   */
  private final transient Map<Broker, ConsumerThread> simpleConsumerThreads = new HashMap<Broker, ConsumerThread>();


  private transient ExecutorService kafkaConsumerExecutor;

  private transient ScheduledExecutorService metadataRefreshExecutor;

  /**
   * The metadata refresh retry counter
   */
  private final transient AtomicInteger retryCounter = new AtomicInteger(0);

  private int timeout = 10000;

  /**
   * Default buffer size is 1M
   */
  private int bufferSize = 1024 * 1024;

  /**
   * Default client id prefix is "Kafka_Simple_Client"
   */
  @NotNull
  private String clientId = "Kafka_Simple_Client";

  /**
   * Interval in between refresh the metadata change(broker change) in milliseconds. Metadata refresh guarantees to
   * automatically reconnect to new broker that are new elected as broker host Disable metadata refresh by setting this
   * to -1 WARN: Turning off the refresh will disable auto reconnect to new broker
   */
  private int metadataRefreshInterval = 30000;

  /**
   * Maximum brokers' metadata refresh retry limit. -1 means unlimited retry
   */
  private int metadataRefreshRetryLimit = -1;

  /**
   * You can setup your particular kafka partitions you want to consume for this consumer client. This can be used to
   * share client and thread and maximize the overall performance. Null or empty value: consumer will create #
   * threads&clients same as # brokers that host the all partitions of the topic Each thread consumes 1(+) partitions
   * from 1 broker
   */
  private Set<KafkaPartition> kps = new HashSet<KafkaPartition>();

  // This map maintains mapping between kafka partition and it's leader broker in realtime monitored by a thread
  private final transient ConcurrentHashMap<KafkaPartition, Broker> partitionToBroker = new ConcurrentHashMap<KafkaPartition, Broker>();

  /**
   * Track offset for each partition, so operator could start from the last serialized state Use ConcurrentHashMap to
   * avoid ConcurrentModificationException without blocking reads when updating in another thread(hashtable or
   * synchronizedmap)
   */
  private final transient ConcurrentHashMap<KafkaPartition, Long> offsetTrack = new ConcurrentHashMap<KafkaPartition, Long>();

  private transient AtomicReference<Throwable> monitorException;
  private transient AtomicInteger monitorExceptionCount;

  @Override
  public void start()
  {
    monitorException = new AtomicReference<Throwable>(null);
    monitorExceptionCount = new AtomicInteger(0);
    super.start();

    // thread to consume the kafka data
    kafkaConsumerExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("kafka-consumer-" + topic + "-%d").build());

    if (metadataRefreshInterval <= 0 || CollectionUtils.isEmpty(kps)) {
      return;
    }

    // background thread to monitor the kafka metadata change
    metadataRefreshExecutor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("kafka-consumer-monitor-" + topic + "-%d").setDaemon(true).build());

    // start one monitor thread to monitor the leader broker change and trigger some action
    metadataRefreshExecutor.scheduleAtFixedRate(new MetaDataMonitorTask(this), 0, metadataRefreshInterval, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close()
  {
    logger.info("Stop all consumer threads");
    for (ConsumerThread ct : simpleConsumerThreads.values()) {
      ct.getThreadItSelf().cancel(true);
    }
    simpleConsumerThreads.clear();
    metadataRefreshExecutor.shutdownNow();
    kafkaConsumerExecutor.shutdownNow();
  }

  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

  public void setClientId(String clientId)
  {
    this.clientId = clientId;
  }

  public void setTimeout(int timeout)
  {
    this.timeout = timeout;
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  public String getClientId()
  {
    return clientId;
  }

  public int getTimeout()
  {
    return timeout;
  }

  public int getMetadataRefreshInterval()
  {
    return metadataRefreshInterval;
  }

  public void setMetadataRefreshInterval(int reconnectInterval)
  {
    this.metadataRefreshInterval = reconnectInterval;
  }

  public int getMetadataRefreshRetryLimit()
  {
    return metadataRefreshRetryLimit;
  }

  public void setMetadataRefreshRetryLimit(int metadataRefreshRetryLimit)
  {
    this.metadataRefreshRetryLimit = metadataRefreshRetryLimit;
  }

  @Override
  protected void commitOffset()
  {
    // the simple consumer offset is kept in the offsetTrack
    // It's better to do server registry for client in the future. Wait for kafka community come up with more
    // sophisticated offset management
    // TODO https://cwiki.apache.org/confluence/display/KAFKA/Inbuilt+Consumer+Offset+Management#
  }

  private String getClientName(String brokerName)
  {
    return clientId + SIMPLE_CONSUMER_ID_SUFFIX + brokerName;
  }

  @Override
  protected Map<KafkaPartition, Long> getCurrentOffsets()
  {
    return offsetTrack;
  }

  public void resetOffset(Map<KafkaPartition, Long> overrideOffset)
  {
    if (overrideOffset == null) {
      return;
    }
    offsetTrack.clear();
    // set offset of the partitions assigned to this consumer
    for (KafkaPartition kp : kps) {
      Long offsetForPar = overrideOffset.get(kp);
      if (offsetForPar != null) {
        offsetTrack.put(kp, offsetForPar);
      }
    }
  }

  public KafkaMeterStats getConsumerStats(Map<KafkaPartition, Long> offsetStats)
  {
    stats.updateOffsets(offsetStats);
    return super.getConsumerStats();
  }

  @Override
  protected void resetPartitionsAndOffset(Set<KafkaPartition> partitionIds, Map<KafkaPartition, Long> startOffset)
  {
    this.kps = partitionIds;
    resetOffset(startOffset);
  }

  protected Throwable getMonitorException()
  {
    return monitorException.get();
  }

  protected int getMonitorExceptionCount()
  {
    return monitorExceptionCount.get();
  }

  /**
   * Task to monitor metadata periodically. This task will detect changes in broker for partition
   * and restart failed consumer threads for the partitions.
   * Monitoring is disabled after metadataRefreshRetryLimit number of failure.
   */
  private class MetaDataMonitorTask implements Runnable
  {
    private final SimpleKafkaConsumer ref;

    private final transient SetMultimap<Broker, KafkaPartition> deltaPositive = HashMultimap.create();

    private MetaDataMonitorTask(SimpleKafkaConsumer ref)
    {
      this.ref = ref;
    }

    @Override
    public void run()
    {
      try {
        monitorMetadata();
        monitorException.set(null);
        monitorExceptionCount.set(0);
      } catch (Throwable ex) {
        logger.error("Exception {}", ex);
        monitorException.set(ex);
        monitorExceptionCount.incrementAndGet();
      }
    }

    /**
     * Monitor kafka topic metadata changes.
     */
    private void monitorMetadata()
    {
      if (isAlive && (metadataRefreshRetryLimit == -1 || retryCounter.get() < metadataRefreshRetryLimit)) {
        logger.debug("{}: Update metadata for topic {}", Thread.currentThread().getName(), topic);
        Map<String, List<PartitionMetadata>> pms = KafkaMetadataUtil.getPartitionsForTopic(brokers, topic);
        if (pms == null) {
          // retrieve metadata fail add retry count and return
          retryCounter.getAndAdd(1);
          return;
        }

        for (Entry<String, List<PartitionMetadata>> pmLEntry : pms.entrySet()) {
          if (pmLEntry.getValue() == null) {
            continue;
          }
          for (PartitionMetadata pm : pmLEntry.getValue()) {
            KafkaPartition kp = new KafkaPartition(pmLEntry.getKey(), topic, pm.partitionId());
            if (!kps.contains(kp)) {
              // Out of this consumer's scope
              continue;
            }
            Broker b = pm.leader();
            if (b == null) {
              logger.info("No Leader broker for Kafka Partition {}. Skipping it for time until new leader is elected", kp.getPartitionId());
              continue;
            }
            Broker oldB = partitionToBroker.put(kp, b);
            if (b.equals(oldB)) {
              continue;
            }
            // add to positive
            deltaPositive.put(b, kp);

            // always update the latest connection information
            stats.updatePartitionStats(kp, pm.leader().id(), pm.leader().host() + ":" + pm.leader().port());
          }
        }

        // remove from map if the thread is done (partitions on this broker has all been reassigned to others(or temporarily not available) for
        // example)
        for (Iterator<Entry<Broker, ConsumerThread>> iterator = simpleConsumerThreads.entrySet().iterator(); iterator.hasNext(); ) {
          Entry<Broker, ConsumerThread> item = iterator.next();
          if (item.getValue().getThreadItSelf().isDone()) {
            iterator.remove();
          }
        }

        for (Broker b : deltaPositive.keySet()) {
          if (!simpleConsumerThreads.containsKey(b)) {
            // start thread for new broker
            ConsumerThread ct = new ConsumerThread(b, deltaPositive.get(b), ref);
            ct.setThreadItSelf(kafkaConsumerExecutor.submit(ct));
            simpleConsumerThreads.put(b, ct);

          } else {
            simpleConsumerThreads.get(b).addPartitions(deltaPositive.get(b));
          }
        }

        deltaPositive.clear();

        // reset to 0 if it reconnect to the broker which has current broker metadata
        retryCounter.set(0);
      }
    }

  }
} // End of SimpleKafkaConsumer
