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
package com.datatorrent.contrib.kafka;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.Pair;

import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;

/**
 * Assumptions: <br/>
 * - assume the value of incoming tuples are not duplicate among all operator partitions. <br/> 
 * - assume one Kafka partition can be written by multiple operator partitions at the same time <br/>
 * - assume the Kafka partition was determined by tuple value itself( not depended on operator partition) <br/>
 * <p/>
 * 
 * Notes: <br/>
 * - the order of data could be changed when replay. 
 * - the data could go to the other partition when replay. For example if the upstream operator failed. <br/>
 * <p/>
 * 
 * Implementation: for each Kafka partition, load minimum last window and the
 * minimum offset of the last window of all operator partitions. And then load
 * the tuples from Kafka based on this minimum offset. When processing tuple, if
 * the window id is less than the minimum last window, just ignore the tuple. If
 * window id equals loaded minimum window id, and tuple equals any of loaded
 * tuple, ignore it. Else, send to Kafka
 * <p/>
 * 
 * @displayName Abstract Tuple Unique Exactly Once Kafka Output
 * @category Messaging
 * @tags output operator
 */
@InterfaceStability.Evolving
public abstract class AbstractTupleUniqueExactlyOnceKafkaOutputOperator<T, K, V>
    extends AbstractKafkaOutputOperator<K, V>
{
  public static final String DEFAULT_CONTROL_TOPIC = "ControlTopic";
  protected transient int partitionNum = 1;

  /**
   * allow client set the partitioner as partitioner may need some attributes
   */
  protected kafka.producer.Partitioner partitioner;

  protected transient int operatorPartitionId;

  protected String controlTopic = DEFAULT_CONTROL_TOPIC;

  //The control info includes the time, use this time to track the head of control info we care.
  protected int controlInfoTrackBackTime = 120000;

  /**
   * max number of offset need to check
   */
  protected int maxNumOffsetsOfControl = 1000;

  protected String controlProducerProperties;
  protected Set<String> brokerSet;
  
  protected transient long currentWindowId;

  /**
   * the map from Kafka partition id to the control offset. this one is
   * checkpointed and as the start offset to load the recovery control
   * information Note: this only keep the information of this operator
   * partition.
   */
  protected transient Map<Integer, Long> partitionToLastControlOffset = Maps.newHashMap();

  /**
   * keep the minimal last window id for recovery. If only one partition
   * crashed, it is ok just use the last window id of this operator partition as
   * the recovery window id If all operator partitions crashed, should use the
   * minimal last window id as the recovery window id, as the data may go to the
   * other partitions. But as the operator can't distinguish which is the case.
   * use the most general one.
   */
  protected transient long minRecoveryWindowId = -2;
  protected transient long maxRecoveryWindowId = -2;

  /**
   * A map from Kafka partition id to lastMessages written to this kafka
   * partition. This information was loaded depends on the
   * RecoveryControlInfo.kafkaPartitionIdToOffset
   */
  protected transient Map<Integer, List<Pair<byte[], byte[]>>> partitionToLastMsgs = Maps.newHashMap();

  /**
   * The messages are assume to written to the kafka partition decided by
   * tupleToKeyValue(T tuple) and partitioner. But it also depended on the
   * system. for example, it could be only one partition when create topic.
   * Don't distinguish kafka partitions if partition is not reliable.
   */
  protected transient Set<Pair<byte[], byte[]>> totalLastMsgs = Sets.newHashSet();

  protected transient RecoveryControlInfo controlInfo = new RecoveryControlInfo();
  protected transient Producer<String, String> controlDataProducer;
  protected transient StringDecoder controlInfoDecoder;

  @Override
  public void setup(OperatorContext context)
  {
    getBrokerSet();
    
    super.setup(context);
    controlInfoDecoder = new StringDecoder(null);

    operatorPartitionId = context.getId();

    controlDataProducer = new Producer<String, String>(createKafkaControlProducerConfig());

    if (partitioner == null) {
      createDefaultPartitioner();
    }

    loadControlData();
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    //we'd better flush the cached tuples, but Kafka 0.8.1 doesn't support flush.
    //keep the control information of this operator partition to control topic
    saveControlData();
  }

  protected void createDefaultPartitioner()
  {
    try {
      String className = (String)getConfigProperties().get(KafkaMetadataUtil.PRODUCER_PROP_PARTITIONER);
      if (className != null) {
        partitioner = (kafka.producer.Partitioner)Class.forName(className).newInstance();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize partitioner", e);
    }
  }

  /**
   * load control data OUTPUT: lastMsgs and partitionToMinLastWindowId
   */
  protected void loadControlData()
  {
    long loadDataTime = System.currentTimeMillis();

    final String clientNamePrefix = getClientNamePrefix();
    Map<Integer, SimpleConsumer> consumers = KafkaUtil.createSimpleConsumers(clientNamePrefix, brokerSet, controlTopic);
    if (consumers == null || consumers.size() != 1) {
      logger.error("The consumer for recovery information was not expected. {}", consumers);
      return;
    }
    final SimpleConsumer consumer = consumers.get(0);
    if (consumer == null) {
      logger.error("No consumer for recovery information.");
      return;
    }

    long latestOffset = KafkaMetadataUtil.getLastOffset(consumer, controlTopic, 0, OffsetRequest.LatestTime(),
        KafkaMetadataUtil.getClientName(clientNamePrefix, controlTopic, 0));
    logger.debug("latestOffsets: {}", latestOffset);
    if (latestOffset <= 0) {
      return;
    }

    int batchMessageSize = 100;
    List<Pair<byte[], byte[]>> messages = Lists.newArrayList();

    boolean isControlMessageEnough = false;
    Map<Integer, RecoveryControlInfo> operatorPartitionIdToLastControlInfo = Maps.newHashMap();

    while (latestOffset > 0 && !isControlMessageEnough) {
      long startOffset = latestOffset - batchMessageSize + 1;
      if (startOffset < 0) {
        startOffset = 0;
      }

      //read offsets as batch and handle them.
      messages.clear();
      KafkaUtil.readMessagesBetween(consumer, KafkaMetadataUtil.getClientName(clientNamePrefix, controlTopic, 0),
          controlTopic, 0, startOffset, latestOffset - 1, messages, 3);
      for (Pair<byte[], byte[]> message : messages) {
        //handle the message; we have to handle all the message.
        RecoveryControlInfo rci = RecoveryControlInfo.fromString((String)controlInfoDecoder.fromBytes(message.second));
        isControlMessageEnough = (loadControlInfoIntermedia(rci, loadDataTime,
            operatorPartitionIdToLastControlInfo) == 0);

        if (isControlMessageEnough) {
          break;
        }
      }

      latestOffset = startOffset - 1;
    }

    loadRecoveryWindowId(operatorPartitionIdToLastControlInfo);
    loadLastMessages(operatorPartitionIdToLastControlInfo);
  }

  /**
   * load the recovery window id. right now use the minimal window id as the
   * recovery window id Different Operator partitions maybe crashed at different
   * window. use the minimal window of all operator partitions as the window for
   * recovery.
   * 
   * @param operatorPartitionIdToLastWindowId
   */
  protected void loadRecoveryWindowId(Map<Integer, RecoveryControlInfo> operatorPartitionIdToLastControlInfo)
  {
    for (RecoveryControlInfo rci : operatorPartitionIdToLastControlInfo.values()) {
      if (minRecoveryWindowId < 0 || rci.windowId < minRecoveryWindowId) {
        minRecoveryWindowId = rci.windowId;
      }
      if (maxRecoveryWindowId < 0 || rci.windowId > maxRecoveryWindowId) {
        maxRecoveryWindowId = rci.windowId;
      }
    }
  }

  /**
   * load control information from intermedia to
   * 
   * @param operatorPartitionIdToLastWindowId
   * @param operatorToKafkaToOffset
   */
  protected void loadLastMessages(Map<Integer, RecoveryControlInfo> operatorPartitionIdToLastControlInfo)
  {
    partitionToLastControlOffset.clear();

    for (Map.Entry<Integer, RecoveryControlInfo> entry : operatorPartitionIdToLastControlInfo.entrySet()) {
      RecoveryControlInfo rci = entry.getValue();
      if (rci.windowId == this.minRecoveryWindowId) {
        //get the minimal offset
        for (Map.Entry<Integer, Long> kafkaPartitionEntry : rci.kafkaPartitionIdToOffset.entrySet()) {
          Long offset = partitionToLastControlOffset.get(kafkaPartitionEntry.getKey());
          if (offset == null || offset > kafkaPartitionEntry.getValue()) {
            partitionToLastControlOffset.put(kafkaPartitionEntry.getKey(), kafkaPartitionEntry.getValue());
          }
        }
      }
    }

    partitionToLastMsgs.clear();

    KafkaUtil.readMessagesAfterOffsetTo(getClientNamePrefix(), brokerSet, getTopic(), partitionToLastControlOffset,
        partitionToLastMsgs);

    loadTotalLastMsgs();
  }

  /**
   * load Total Last Messages from partitionToLastMsgs;
   */
  protected void loadTotalLastMsgs()
  {
    totalLastMsgs.clear();
    if (partitionToLastMsgs == null || partitionToLastMsgs.isEmpty()) {
      return;
    }
    for (List<Pair<byte[], byte[]>> msgs : partitionToLastMsgs.values()) {
      totalLastMsgs.addAll(msgs);
    }
  }

  protected int loadControlInfoIntermedia(RecoveryControlInfo controlInfo, long loadDataTime,
      Map<Integer, RecoveryControlInfo> operatorPartitionIdToLastControlInfo)
  {
    if (controlInfo.generateTime + controlInfoTrackBackTime < loadDataTime) {
      return 0;
    }

    //The record should be in ascent order, so the later should override the previous
    operatorPartitionIdToLastControlInfo.put(controlInfo.partitionIdOfOperator, controlInfo);

    return 1;
  }

  /**
   * Current implementation we can get the number of operator partitions. So we
   * we use the controlInfoTrackBackTime to control the trace back of control
   * information.
   * 
   * @param controlInfo
   * @param loadDataTime
   * @param operatorPartitionIdToLastWindowId
   * @param operatorToKafkaToOffset
   * @return 0 if control information is enough and don't need to load any more
   */
  protected int loadControlInfoIntermedia(RecoveryControlInfo controlInfo, long loadDataTime,
      Map<Integer, Long> operatorPartitionIdToLastWindowId, Map<Integer, Map<Integer, Long>> operatorToKafkaToOffset)
  {
    if (controlInfo.generateTime + controlInfoTrackBackTime < loadDataTime) {
      return 0;
    }

    //The record should be in ascent order, so the later should override the previous
    operatorPartitionIdToLastWindowId.put(controlInfo.partitionIdOfOperator, controlInfo.windowId);
    operatorToKafkaToOffset.put(controlInfo.partitionIdOfOperator, controlInfo.kafkaPartitionIdToOffset);

    return 1;
  }

  /**
   * save the control data. each operator partition only save its control data
   */
  protected void saveControlData()
  {
    controlInfo.generateTime = System.currentTimeMillis();
    controlInfo.partitionIdOfOperator = operatorPartitionId;
    controlInfo.windowId = this.currentWindowId;
    if (controlInfo.kafkaPartitionIdToOffset == null) {
      controlInfo.kafkaPartitionIdToOffset = Maps.newHashMap();
    } else {
      controlInfo.kafkaPartitionIdToOffset.clear();
    }
    KafkaMetadataUtil.getLastOffsetsTo(getClientNamePrefix(), brokerSet, getTopic(),
        controlInfo.kafkaPartitionIdToOffset);

    //send to control topic
    controlDataProducer.send(new KeyedMessage<String, String>(getControlTopic(), null, 0, controlInfo.toString()));
  }

  protected String getClientNamePrefix()
  {
    return getClass().getName().replace('$', '.');
  }


  protected Set<String> getBrokerSet()
  {
    if (brokerSet == null) {
      brokerSet = Sets.newHashSet((String)getConfigProperties().get(KafkaMetadataUtil.PRODUCER_PROP_BROKERLIST));
    }
    return brokerSet;
  }

  /**
   * This input port receives tuples that will be written out to Kafka.
   */
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * separate to a method to give sub-class chance to override
   * 
   * @param tuple
   */
  protected void processTuple(T tuple)
  {
    Pair<K, V> keyValue = tupleToKeyValue(tuple);
    final int pid = getPartitionKey(keyValue.first);

    if (!skipTuple(pid, keyValue)) {
      getProducer().send(new KeyedMessage<K, V>(getTopic(), keyValue.first, pid, keyValue.second));
      sendCount++;
    }
  }

  protected boolean skipTuple(int partitionId, Pair<K, V> msg)
  {
    if (currentWindowId <= minRecoveryWindowId) {
      return true;
    }
    if (currentWindowId > maxRecoveryWindowId + 1) {
      return false;
    }

    return isDuplicateTuple(partitionId, msg);
  }

  protected boolean isDuplicateTuple(int partitionId, Pair<K, V> msg)
  {
    Collection<Pair<byte[], byte[]>> lastMsgs = partitionToLastMsgs.get(partitionId);

    //check depended on the partition only
    if (lastMsgs == null || lastMsgs.isEmpty()) {
      lastMsgs = totalLastMsgs;
    }

    for (Pair<byte[], byte[]> cachedMsg : lastMsgs) {
      if (equals(cachedMsg, msg)) {
        return true;
      }
    }
    return false;

  }

  protected boolean equals(Pair<byte[], byte[]> cachedMsg, Pair<K, V> msg)
  {
    if (cachedMsg.first == null ^ msg.first == null) {
      return false;
    }
    if (cachedMsg.second == null ^ msg.second == null) {
      return false;
    }

    if (cachedMsg.first == null && msg.first == null && cachedMsg.second == null && msg.second == null) {
      return true;
    }

    if (!equals(cachedMsg.first, msg.first)) {
      return false;
    }

    return equals(cachedMsg.second, msg.second);
  }

  /**
   * 
   * @param bytes
   * @param value
   * @return
   */
  protected abstract <M> boolean equals(byte[] bytes, M value);

  /**
   * get the partition key. for 0.8.1, If a partition key is provided it will
   * override the key for the purpose of partitioning but will not be stored.
   * 
   * @return
   */
  protected int getPartitionKey(K key)
  {
    if (partitioner != null) {
      return partitioner.partition(key, partitionNum);
    }

    if (key != null) {
      return key.hashCode();
    }

    //stick to the Kafka partition, so can't use round robbin
    return 0;
  }

  /**
   * setup the configuration for control producer
   * 
   * @return
   */
  protected ProducerConfig createKafkaControlProducerConfig()
  {
    if (controlProducerProperties == null || controlProducerProperties.isEmpty()) {
      controlProducerProperties = getProducerProperties();
    }

    Properties prop = new Properties();
    for (String propString : controlProducerProperties.split(",")) {
      if (!propString.contains("=")) {
        continue;
      }
      String[] keyVal = StringUtils.trim(propString).split("=");
      prop.put(StringUtils.trim(keyVal[0]), StringUtils.trim(keyVal[1]));
    }

    //only support String encoder now, overwrite
    prop.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    prop.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");

    Properties configProperties = this.getConfigProperties();
    configProperties.putAll(prop);

    return new ProducerConfig(configProperties);
  }

  /**
   * Tell the operator how to convert a input tuple to a kafka key value pair
   * 
   * @param tuple
   * @return A kafka key value pair.
   */
  protected abstract Pair<K, V> tupleToKeyValue(T tuple);

  public kafka.producer.Partitioner getPartitioner()
  {
    return partitioner;
  }

  public void setPartitioner(kafka.producer.Partitioner partitioner)
  {
    this.partitioner = partitioner;
  }

  public String getControlTopic()
  {
    return controlTopic;
  }

  public void setControlTopic(String controlTopic)
  {
    this.controlTopic = controlTopic;
  }

  public String getControlProducerProperties()
  {
    return controlProducerProperties;
  }

  public void setControlProducerProperties(String controlProducerProperties)
  {
    this.controlProducerProperties = controlProducerProperties;
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractExactlyOnceKafkaOutputOperator.class);

  /**
   * This class used to keep the recovery information
   *
   */
  protected static class RecoveryControlInfo
  {
    protected static final String SEPERATOR = "#";
    protected int partitionIdOfOperator;
    protected long generateTime;
    protected long windowId;
    protected Map<Integer, Long> kafkaPartitionIdToOffset;
    //( operatorPartitionId => ( lastWindowId, (KafkaPartitionId => offset) ) )

    @Override
    public String toString()
    {
      StringBuilder sb = new StringBuilder();
      sb.append(partitionIdOfOperator).append(SEPERATOR).append(generateTime).append(SEPERATOR).append(windowId);
      sb.append(SEPERATOR).append(kafkaPartitionIdToOffset);
      return sb.toString();
    }

    public static RecoveryControlInfo fromString(String str)
    {
      if (str == null || str.isEmpty()) {
        throw new IllegalArgumentException("Input parameter is null or empty.");
      }
      String[] fields = str.split(SEPERATOR);
      if (fields == null || fields.length != 4) {
        throw new IllegalArgumentException(
            "Invalid input String: \"" + str + "\", " + "expected fields seperated by '" + SEPERATOR + "'");
      }

      RecoveryControlInfo rci = new RecoveryControlInfo();
      rci.partitionIdOfOperator = Integer.valueOf(fields[0]);
      rci.generateTime = Long.valueOf(fields[1]);
      rci.windowId = Long.valueOf(fields[2]);

      String mapString = fields[3].trim();
      if (mapString.startsWith("{") && mapString.endsWith("}")) {
        mapString = mapString.substring(1, mapString.length() - 1);
      }
      Map<String, String> idToOffsetAsString = Splitter.on(",").withKeyValueSeparator("=").split(mapString);
      rci.kafkaPartitionIdToOffset = Maps.newHashMap();
      for (Map.Entry<String, String> entry : idToOffsetAsString.entrySet()) {
        rci.kafkaPartitionIdToOffset.put(Integer.valueOf(entry.getKey()), Long.valueOf(entry.getValue()));
      }
      return rci;
    }
  }

}
