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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Kafka output operator with exactly once processing semantics.
 *<br>
 *
 * <p>
 * <b>Recovery handling</b>
 * <li> Offsets of the Kafka partitions are stored in the WindowDataManager at the endWindow</li>
 * <li> During recovery,
 * <ul>
 *    <li>Partially written Streaming Window before the crash is constructed. ( Explained below ) </li>
 *    <li>Tuples from the completed Streaming Window's are skipped </li>
 *    <li>Tuples coming for the partially written Streaming Window are skipped.
 *       (No assumption is made on the order and the uniqueness of the tuples) </li>
 *  </ul>
 *  </li>
 *</p>
 *
 * <p>
 * <b>Partial Window Construction</b>
 * <li> Operator uses the Key in the Kafka message, which is not available for use by the operator users.</li>
 * <li> Key is used to uniquely identify the message written by the particular instance of this operator.</li>
 *    This allows multiple writers to same Kafka partitions. Format of the key is "APPLICATTION_ID#OPERATOR_ID".
 * <li>During recovery Kafka partitions are read between the latest offset and the last written offsets.</li>
 *<li>All the tuples written by the particular instance is kept in the Map</li>
 *</p>
 *
 * <p>
 * <b>Limitations</b>
 * <li> Key in the Kafka message is reserved for Operator's use </li>
 * <li> During recovery, operator needs to read tuples between 2 offsets, if there are lot of data to be read, Operator may
 *    appear to be blocked to the Stram and can kill the operator. </li>
 *</p>
 *
 * @displayName Kafka Single Port Exactly Once Output(0.9.0)
 * @category Messaging
 * @tags output operator
 *
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class KafkaSinglePortExactlyOnceOutputOperator<T> extends AbstractKafkaOutputOperator<String, T>
    implements Operator.CheckpointNotificationListener
{
  private transient String key;
  private transient String appName;
  private transient Integer operatorId;
  private transient Long windowId;
  private transient Map<T, Integer> partialWindowTuples = new HashMap<>();
  private transient KafkaConsumer consumer;

  private WindowDataManager windowDataManager = new FSWindowDataManager();
  private final int KAFKA_CONNECT_ATTEMPT = 10;
  private final String KEY_SEPARATOR = "#";

  private final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
  private final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      sendTuple(tuple);
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    this.operatorId = context.getId();
    this.windowDataManager.setup(context);
    this.appName = context.getValue(Context.DAGContext.APPLICATION_NAME);
    this.key = appName + KEY_SEPARATOR + (new Integer(operatorId));
    this.consumer = KafkaConsumerInit();
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;

    if (windowId == windowDataManager.getLargestRecoveryWindow()) {
      rebuildPartialWindow();
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    try {
      windowDataManager.deleteUpTo(operatorId, windowId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
  }

  @Override
  public void teardown()
  {
    consumer.close();
    super.teardown();
  }

  @Override
  public void endWindow()
  {
    if (!partialWindowTuples.isEmpty() && windowId > windowDataManager.getLargestRecoveryWindow()) {
      throw new RuntimeException("Violates Exactly once. Not all the tuples received after operator reset.");
    }

    // Every tuples should be written before the offsets are stored in the window data manager.
    getProducer().flush();

    try {
      this.windowDataManager.save(getPartitionsAndOffsets(true), operatorId, windowId);
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  private boolean doesKeyBelongsToThisInstance(int operatorId, String key)
  {
    String[] split = key.split(KEY_SEPARATOR);

    if (split.length != 2) {
      return false;
    }

    if ((Integer.parseInt(split[1]) == operatorId) && (split[0].equals(appName))) {
      return true;
    }

    return false;
  }

  private boolean alreadyInKafka(T message)
  {
    if ( windowId <= windowDataManager.getLargestRecoveryWindow() ) {
      return true;
    }

    if (partialWindowTuples.containsKey(message)) {

      Integer val = partialWindowTuples.get(message);

      if ( val == 0 ) {
        return false;
      } else if ( val == 1 ) {
        partialWindowTuples.remove(message);
      } else {
        partialWindowTuples.put(message, val - 1);
      }

      return true;
    }

    return false;
  }

  private Map<Integer,Long> getPartitionsAndOffsets(boolean latest) throws ExecutionException, InterruptedException
  {
    List<PartitionInfo> partitionInfoList = consumer.partitionsFor(getTopic());
    List<TopicPartition> topicPartitionList = new java.util.ArrayList<>();

    for ( PartitionInfo partitionInfo: partitionInfoList) {
      topicPartitionList.add(new TopicPartition(getTopic(), partitionInfo.partition()) );
    }

    Map<Integer,Long> parttionsAndOffset = new HashMap<>();
    consumer.assign(topicPartitionList);

    for ( PartitionInfo partitionInfo: partitionInfoList) {

      try {

        TopicPartition topicPartition = new TopicPartition(getTopic(), partitionInfo.partition());
        if (latest) {
          consumer.seekToEnd(topicPartition);
        } else {
          consumer.seekToBeginning(topicPartition);
        }
        parttionsAndOffset.put(partitionInfo.partition(), consumer.position(topicPartition));

      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    return parttionsAndOffset;
  }

  private void rebuildPartialWindow()
  {
    logger.info("Rebuild the partial window after " + windowDataManager.getLargestRecoveryWindow());

    Map<Integer,Long> storedOffsets;
    Map<Integer,Long> currentOffsets;

    try {
      storedOffsets = (Map<Integer,Long>)this.windowDataManager.load(operatorId, windowId);
      currentOffsets = getPartitionsAndOffsets(true);
    } catch (IOException | ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (currentOffsets == null) {
      logger.debug("No tuples found while building partial window " + windowDataManager.getLargestRecoveryWindow());
      return;
    }

    if (storedOffsets == null) {

      logger.debug("Stored offset not available, seeking to the beginning of the Kafka Partition.");

      try {
        storedOffsets = getPartitionsAndOffsets(false);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    List<TopicPartition> topicPartitions = new ArrayList<>();

    for (Map.Entry<Integer,Long> entry: currentOffsets.entrySet()) {

      topicPartitions.add(new TopicPartition(getTopic(), entry.getKey()));
    }

    consumer.assign(topicPartitions);

    for (Map.Entry<Integer,Long> entry: currentOffsets.entrySet()) {

      Long storedOffset = 0L;
      Integer currentPartition = entry.getKey();
      Long currentOffset = entry.getValue();

      if (storedOffsets.containsKey(currentPartition)) {
        storedOffset = storedOffsets.get(currentPartition);
      }

      if (storedOffset >= currentOffset) {
        continue;
      }

      try {
        consumer.seek(new TopicPartition(getTopic(), currentPartition), storedOffset);
      } catch (Exception ex) {
        logger.info("Rebuilding of the partial window is not complete, exactly once recovery is not possible.");
        throw new RuntimeException(ex);
      }

      int kafkaAttempt = 0;

      while ( true ) {

        ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

        if (consumerRecords.count() == 0) {
          if (kafkaAttempt++ == KAFKA_CONNECT_ATTEMPT) {
            break;
          }
        } else {
          kafkaAttempt = 0;
        }

        boolean crossedBoundary = false;

        for (ConsumerRecord consumerRecord : consumerRecords) {

          if (!doesKeyBelongsToThisInstance(operatorId, (String)consumerRecord.key())) {
            continue;
          }

          T value = (T)consumerRecord.value();

          if ( partialWindowTuples.containsKey(value)) {
            Integer count = partialWindowTuples.get(value);
            partialWindowTuples.put(value, count + 1);
          } else {
            partialWindowTuples.put(value, 1);
          }

          if (consumerRecord.offset() >= currentOffset) {
            crossedBoundary = true;
            break;
          }
        }

        if (crossedBoundary) {
          break;
        }
      }
    }
  }

  private KafkaConsumer KafkaConsumerInit()
  {
    Properties props = new Properties();

    props.put(BOOTSTRAP_SERVERS_CONFIG, getProperties().get(BOOTSTRAP_SERVERS_CONFIG));
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);

    return new KafkaConsumer<>(props);
  }

  protected void sendTuple(T tuple)
  {
    if ( alreadyInKafka(tuple) ) {
      return;
    }

    getProducer().send(new ProducerRecord<>(getTopic(), key, tuple),new Callback()
    {
      public void onCompletion(RecordMetadata metadata, Exception e)
      {
        if (e != null) {
          logger.info("Wrting to Kafka failed with an exception {}" + e.getMessage());
          throw new RuntimeException(e);
        }
      }
    });
  }

  private static final Logger logger = LoggerFactory.getLogger(KafkaSinglePortExactlyOnceOutputOperator.class);
}

