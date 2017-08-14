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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.Pair;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;

/**
 * This is a base implementation of a Kafka output operator,
 * which, in most cases, guarantees to send tuples to Kafka MQ only once.&nbsp;
 * Subclasses should implement the methods for converting tuples into a format appropriate for Kafka.
 * <p>
 * Assuming messages kept in kafka are ordered by either key or value or keyvalue pair
 * (For example, use timestamps as key), this Kafka OutputOperator always retrieve the last message from MQ as initial offset.
 *  So that replayed message wouldn't be sent to kafka again.
 *
 * This is not "perfect exact once" in 2 cases:
 * 1 Multiple producers produce messages to same kafka partition
 * 2 You have same message sent out and before kafka synchronized this message among all the brokers, the operator is
 * started again.
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: One input port<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * configProperties<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from has to implement 2 methods:<br>
 * tupleToKeyValue() to convert input tuples to kafka key value objects<br>
 * compareToLastMsg() to compare incoming tuple with the last received msg in kafka so that the operator could skip the received ones<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * </p>
 *
 * @displayName Abstract Exactly Once Kafka Output
 * @category Messaging
 * @tags output operator
 *
 * @since 1.0.2
 */
public abstract class AbstractExactlyOnceKafkaOutputOperator<T, K, V> extends AbstractKafkaOutputOperator<K, V>
{
  private Map<Integer, Pair<byte[], byte[]>>  lastMsgs;

  private transient  Partitioner partitioner;

  private transient int partitionNum = 1;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    try {
      String className = (String)getConfigProperties().get(KafkaMetadataUtil.PRODUCER_PROP_PARTITIONER);
      if (className != null) {
        partitioner = (Partitioner)Class.forName(className).newInstance();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize partitioner", e);
    }
    //read last message from kafka
    initializeLastProcessingOffset();
  }

  /**
   * This input port receives tuples that will be written out to Kafka.
   */
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      Pair<K, V> keyValue = tupleToKeyValue(tuple);
      int pid = 0;

      if (partitioner != null) {
        pid = partitioner.partition(keyValue.first, partitionNum);
      }

      Pair<byte[], byte[]> lastMsg = lastMsgs.get(pid);
      if (lastMsg == null || compareToLastMsg(keyValue, lastMsg) > 0) {
        getProducer().send(new KeyedMessage<K, V>(getTopic(), keyValue.first, keyValue.second));
        sendCount++;
      } else {
        // ignore tuple because kafka has already had the tuple
        logger.debug("Ingore tuple " + tuple);
        return;
      }
    }
  };

  private void initializeLastProcessingOffset()
  {
    // read last received kafka message
    TopicMetadata tm = KafkaMetadataUtil.getTopicMetadata(Sets.newHashSet((String)getConfigProperties().get(KafkaMetadataUtil.PRODUCER_PROP_BROKERLIST)), this.getTopic());

    if (tm == null) {
      throw new RuntimeException("Failed to retrieve topic metadata");
    }

    partitionNum = tm.partitionsMetadata().size();

    lastMsgs = new HashMap<Integer, Pair<byte[],byte[]>>(partitionNum);

    for (PartitionMetadata pm : tm.partitionsMetadata()) {

      String leadBroker = pm.leader().host();
      int port = pm.leader().port();
      String clientName = this.getClass().getName().replace('$', '.') + "_Client_" + tm.topic() + "_" + pm.partitionId();
      SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);

      long readOffset = KafkaMetadataUtil.getLastOffset(consumer, tm.topic(), pm.partitionId(), kafka.api.OffsetRequest.LatestTime(), clientName);

      FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(tm.topic(), pm.partitionId(), readOffset - 1, 100000).build();

      FetchResponse fetchResponse = consumer.fetch(req);
      for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(tm.topic(), pm.partitionId())) {

        Message m = messageAndOffset.message();

        ByteBuffer payload = m.payload();
        ByteBuffer key = m.key();
        byte[] valueBytes = new byte[payload.limit()];
        byte[] keyBytes = new byte[key.limit()];
        payload.get(valueBytes);
        key.get(keyBytes);
        lastMsgs.put(pm.partitionId(), new Pair<byte[], byte[]>(keyBytes, valueBytes));
      }
    }
  }

  /**
   * compare the incoming tuple with the last received message in kafka.
   *
   * @param tupleKeyValue
   * @param lastReceivedKeyValue
   * @return <=0 if tupleKeyValue is supposed to be before lastReceivedKeyValue
   *          >0 if tupleKeyValue is after the lastReceivedKeyValue
   */
  protected abstract int compareToLastMsg(Pair<K, V> tupleKeyValue, Pair<byte[], byte[]> lastReceivedKeyValue);

  /**
   * Tell the operator how to convert a input tuple to a kafka key value pair
   * @param tuple
   * @return A kafka key value pair.
   */
  protected abstract Pair<K, V> tupleToKeyValue(T tuple);

  private static final Logger logger = LoggerFactory.getLogger(AbstractExactlyOnceKafkaOutputOperator.class);

}
