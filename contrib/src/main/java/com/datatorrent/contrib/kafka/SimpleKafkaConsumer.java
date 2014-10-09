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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * Simple kafka consumer adaptor used by kafka input operator
 * Properties:<br>
 * <b>timeout</b>: Timeout for connection and ping <br>
 * <b>bufferSize</b>: buffer size of the consumer <br>
 * <b>clientId</b>: client id of the consumer <br>
 * <b>partitionIds</b>: partition id that the consumer want to consume <br>
 * <li> (-1): create #partition threads and consumers to read the topic from different partitions in parallel</li>
 * <br>
 * <b>metadataRefreshInterval</b>: The interval that the monitor thread use to monitor the broker leadership change <br>
 * <b>metadataRetrievalRetry</b>: Maximum retry times for metadata retrieval failures<br>
 * default value is 3 <br>
 * -1: always retry <br>
 * <br>
 *
 * Load balance: <br>
 * <li>Every consumer only connects to leader broker for particular partition once it's created</li>
 * <li>Once leadership change detected(leader broker failure, or server-side reassignment), it switches to the new leader broker</li>
 * <li>For server-side leadership change, see kafka-preferred-replica-election.sh and kafka-reassign-partitions.sh</li>
 * <li>There is ONE separate thread to monitor the leadership for all the partitions of the topic at every #metadataRefreshInterval milliseconds</li>
 * <br>
 * <br>
 * Kafka broker failover: <br>
 * <li>Once broker failure is detected, it waits #metadataRefreshInterval to reconnect to the new leader broker </li>
 * <li>If there are consecutive #metadataRetrievalRetry failures to retrieve the metadata for the topic. It will stop consuming the partition</li>
 * <br>
 *
 * @since 0.9.0
 */
public class SimpleKafkaConsumer extends KafkaConsumer
{

  public SimpleKafkaConsumer()
  {
    super();
  }

  public SimpleKafkaConsumer(String topic, int timeout, int bufferSize, String clientId)
  {
    this(topic, timeout, bufferSize, clientId, null);
  }

  public SimpleKafkaConsumer(String topic, int timeout, int bufferSize, String clientId, Set<Integer> partitionIds)
  {
    super(topic);
    this.timeout = timeout;
    this.bufferSize = bufferSize;
    this.clientId = clientId;
    this.partitionIds = partitionIds;
  }

  public SimpleKafkaConsumer(Set<String> brokerList, String topic, int timeout, int bufferSize, String clientId, Set<Integer> partitionIds)
  {
    super(brokerList, topic);
    this.timeout = timeout;
    this.bufferSize = bufferSize;
    this.clientId = clientId;
    this.partitionIds = partitionIds;
  }

  private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

  /**
   * Track thread for each partition, clean the resource if necessary
   */
  private final transient HashMap<Integer, AtomicReference<SimpleConsumer>> simpleConsumerThreads = new HashMap<Integer, AtomicReference<SimpleConsumer>>();

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
   * interval in between reconnect if one kafka broker goes down in milliseconds
   * if metadataRefreshInterval < 0 it will never refresh the metadata
   * WARN: Turning off the refresh will disable failover to new broker
   */
  private int metadataRefreshInterval = 30000;


  /**
   * Maximum brokers' metadata refresh retry limit
   * -1 means unlimited retry
   */
  private int metadataRefreshRetryLimit = -1;

  /**
   * You can setup your particular partitionID you want to consume with *simple
   * kafka consumer*. Use this to maximize the distributed performance.
   * By default it's -1 which means #partitionSize anonymous threads will be
   * created to consume tuples from different partition
   */
  private Set<Integer> partitionIds = new HashSet<Integer>();


  /**
   * Track offset for each partition, so operator could start from the last serialized state
   * Use ConcurrentHashMap to avoid ConcurrentModificationException without blocking reads when updating in another thread(hashtable or synchronizedmap)
   */
  private final ConcurrentHashMap<Integer, Long> offsetTrack = new ConcurrentHashMap<Integer, Long>();

  @Override
  public void create()
  {
    super.create();
    List<PartitionMetadata> partitionMetaList = KafkaMetadataUtil.getPartitionsForTopic(brokerSet, topic);
    boolean defaultSelect = (partitionIds == null) || (partitionIds.size() == 0);

    // if partition ids are null or not specified , find all the partitions for
    // the specific topic else create the consumers of specified partition ids
    for (PartitionMetadata part : partitionMetaList) {
      final String clientName = getClientName(part.partitionId());
      if (defaultSelect || partitionIds.contains(part.partitionId())) {
        logger.info("Connecting to {}:{} [timeout:{}, buffersize:{}, clientId: {}]", part.leader().host(), part.leader().port(), timeout, bufferSize, clientName);
        simpleConsumerThreads.put(part.partitionId(), new AtomicReference<SimpleConsumer>(new SimpleConsumer(part.leader().host(), part.leader().port(), timeout, bufferSize, clientName)));
      }
    }

  }

  @Override
  public void start()
  {
    super.start();

    // thread to consume the kafka data
    kafkaConsumerExecutor = Executors.newFixedThreadPool(simpleConsumerThreads.size(), new ThreadFactoryBuilder().setNameFormat("kafka-consumer-" + topic + "-%d").build());

    // background thread to monitor the kafka metadata change
    metadataRefreshExecutor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
    .setNameFormat("kafka-consumer-monitor-" + topic + "-%d").setDaemon(true).build());

    // start one monitor thread to monitor the leader broker change and trigger some action
    if (metadataRefreshInterval > 0) {
      metadataRefreshExecutor.scheduleAtFixedRate(new Runnable() {

        @Override
        public void run()
        {
          if (isAlive && (metadataRefreshRetryLimit == -1 || retryCounter.get() < metadataRefreshRetryLimit)) {
            logger.debug("{}: Update metadata for topic {}", Thread.currentThread().getName(), topic);
            List<PartitionMetadata> pms = KafkaMetadataUtil.getPartitionsForTopic(brokerSet, topic);
            if (pms == null) {
              // retrieve metadata fail
              retryCounter.getAndAdd(1);
            }
            Set<String> newBrokerSet = new HashSet<String>();
            for (PartitionMetadata pm : pms) {
              for (Broker b : pm.isr()) {
                newBrokerSet.add(b.host() + ":" + b.port());
              }
              int pid = pm.partitionId();
              if (simpleConsumerThreads.containsKey(pid)) {
                SimpleConsumer sc = simpleConsumerThreads.get(pid).get();
                if (sc != null && sc.host().equals(pm.leader().host()) && sc.port() == pm.leader().port()) {
                  continue;
                }
                // clean the consumer to reestablish the new connection
                logger.info("Detected leader broker change, reconnecting to {}:{} for partition {}", pm.leader().host(), pm.leader().port(), pid);
                cleanAndSet(pid, new SimpleConsumer(pm.leader().host(), pm.leader().port(), timeout, bufferSize, getClientName(pid)));
              }
            }
            brokerSet = newBrokerSet;
            // reset to 0 if it reconnect to the broker which has current broker metadata
            retryCounter.set(0);
          }
        }
      }, 0, metadataRefreshInterval, TimeUnit.MILLISECONDS);
    }



    for (final Integer pid : simpleConsumerThreads.keySet()) {
      //  initialize the stats snapshot for this partition
      statsSnapShot.mark(pid, 0);
      final String clientName = getClientName(pid);
      kafkaConsumerExecutor.submit(new Runnable() {

        AtomicReference<SimpleConsumer> csInThreadRef = simpleConsumerThreads.get(pid);

        @Override
        public void run()
        {
          // read either from beginning of the broker or last offset committed by the operator
          long offset = 0L;
          if(offsetTrack.get(pid)!=null){
            //start from recovery
            offset = offsetTrack.get(pid);
            logger.debug("Partition {} offset {}", pid, offset);
          } else {
            long startOffsetReq = initialOffset.equalsIgnoreCase("earliest")? OffsetRequest.EarliestTime() : OffsetRequest.LatestTime();
            logger.debug("Partition {} initial offset {} {}", pid, startOffsetReq, initialOffset);
            offset = KafkaMetadataUtil.getLastOffset(csInThreadRef.get(), topic, pid, startOffsetReq, clientName);
          }
          try {
            // stop consuming only when the consumer container is stopped or the metadata can not be refreshed
            while (isAlive && (metadataRefreshRetryLimit == -1 || retryCounter.get() < metadataRefreshRetryLimit)) {

              try {
                FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, pid, offset, bufferSize).build();
                SimpleConsumer sc = csInThreadRef.get();
                if (sc == null) {
                  if (metadataRefreshInterval > 0) {
                    Thread.sleep(metadataRefreshInterval + 1000);
                  } else {
                    Thread.sleep(100);
                  }
                }
                FetchResponse fetchResponse = csInThreadRef.get().fetch(req);

                if (fetchResponse.hasError() && fetchResponse.errorCode(topic, pid) == ErrorMapping.OffsetOutOfRangeCode()) {
                  // If OffsetOutOfRangeCode happen, it means all msgs have been consumed, clean the consumer and return
                  cleanAndSet(pid, null);
                  return;
                } else if (fetchResponse.hasError()) {
                  // If error happen, assume
                  throw new Exception("Fetch message error, try to reconnect to broker");
                }

                for (MessageAndOffset msg : fetchResponse.messageSet(topic, pid)) {
                  offset = msg.nextOffset();
                  putMessage(pid, msg.message());
                }
                offsetTrack.put(pid, offset);

              } catch (Exception e) {
                logger.error("The consumer encounters an exception. Close the connection to partition {} ", pid, e);
                cleanAndSet(pid, null);
              }
            }
          } finally {
            // close the consumer
            if(csInThreadRef.get()!=null){
              csInThreadRef.get().close();
            }
            logger.info("Exit the consumer thread for partition {} ", pid);
          }
        }

      });
    }
  }

  private void cleanAndSet(Integer pid, SimpleConsumer newConsumer)
  {
    SimpleConsumer sc = simpleConsumerThreads.get(pid).getAndSet(newConsumer);
    if (sc != null) {
      logger.info("Close old connection to partition {}", pid);
      sc.close();
    }
  }

  @Override
  public void close()
  {
    logger.info("Stop all consumer threads");
    for(AtomicReference<SimpleConsumer> simConsumerRef : simpleConsumerThreads.values()){
      if(simConsumerRef.get()!=null) {
        simConsumerRef.get().close();
      }
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
  protected KafkaConsumer cloneConsumer(Set<Integer> partitionIds, Map<Integer, Long> startOffset)
  {
    // create different client for same partition
    SimpleKafkaConsumer  skc = new SimpleKafkaConsumer(brokerSet, topic, timeout, bufferSize, clientId, partitionIds);
    skc.initialOffset = this.initialOffset;
    skc.resetOffset(startOffset);
    return skc;
  }

  @Override
  protected KafkaConsumer cloneConsumer(Set<Integer> partitionIds){
    return cloneConsumer(partitionIds, null);
  }

  @Override
  protected void commitOffset()
  {
    // the simple consumer offset is kept in the offsetTrack
    // It's better to do server registry for client in the future. Wait for kafka community come up with more sophisticated offset management
    //TODO https://cwiki.apache.org/confluence/display/KAFKA/Inbuilt+Consumer+Offset+Management#
  }


  private String getClientName(int pid){
    return clientId + SIMPLE_CONSUMER_ID_SUFFIX + pid;
  }

  @Override
  protected Map<Integer, Long> getCurrentOffsets()
  {
    return offsetTrack;
  }

  private void resetOffset(Map<Integer, Long> overrideOffset){

    if(overrideOffset == null){
      return;
    }
    offsetTrack.clear();
    // set offset of the partitions assigned to this consumer
    for (Integer pid: partitionIds) {
      Long offsetForPar = overrideOffset.get(pid);
      if (offsetForPar != null) {
        offsetTrack.put(pid, offsetForPar);
      }
    }
  }

  @Override
  public KafkaMeterStats getConsumerStats()
  {
    KafkaMeterStats stat = super.getConsumerStats();
    stat.putOffsets(offsetTrack);
    return stat;
  }


} // End of SimpleKafkaConsumer
