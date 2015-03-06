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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.Pair;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DefaultSerializer(JavaSerializer.class)
class KafkaPair <F, S> extends Pair<F, S>
{
  public KafkaPair(F first, S second)
  {
    super(first, second);
  }
}

/**
 * This is a base implementation of a Kafka input operator, which consumes data from Kafka message bus.&nbsp; Subclasses
 * should implement the method for emitting tuples to downstream operators.
 * <p>
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
 * Each operator can only consume 1 topic from multiple clusters and partitions<br>
 * If you want partitionable operator refer to {@link AbstractPartitionableKafkaInputOperator} <br>
 * </p>
 *
 * @displayName Abstract Kafka Input
 * @category Messaging
 * @tags input operator
 *
 * @since 0.3.2
 */
public abstract class AbstractKafkaInputOperator<K extends KafkaConsumer> implements InputOperator, ActivationListener<OperatorContext>, CheckpointListener
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaInputOperator.class);

  @Min(1)
  private int maxTuplesPerWindow = Integer.MAX_VALUE;
  private transient int emitCount = 0;
  protected IdempotentStorageManager idempotentStorageManager;
  protected transient long currentWindowId;
  protected transient int operatorId;
  protected final transient Map<KafkaPartition, KafkaPair<Long, Integer>> currentWindowRecoveryState;
  protected transient Map<KafkaPartition, Long> offsetStats = new HashMap<KafkaPartition, Long>();
  private transient OperatorContext context = null;
  @NotNull
  @Valid
  protected KafkaConsumer consumer = new SimpleKafkaConsumer();

  public AbstractKafkaInputOperator()
  {
    idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
    currentWindowRecoveryState = new HashMap<KafkaPartition, KafkaPair<Long, Integer>>();
  }
  /**
   * Any concrete class derived from KafkaInputOperator has to implement this method so that it knows what type of
   * message it is going to send to Malhar in which output port.
   *
   * @param message
   */
  protected abstract void emitTuple(Message message);

  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    if(!(getConsumer() instanceof SimpleKafkaConsumer))
      idempotentStorageManager = new IdempotentStorageManager.NoopIdempotentStorageManager();
    logger.debug("consumer {} topic {} cacheSize {}", consumer, consumer.getTopic(), consumer.getCacheSize());
    consumer.create();
    this.context = context;
    operatorId = context.getId();
    idempotentStorageManager.setup(context);
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    idempotentStorageManager.teardown();
    consumer.teardown();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (windowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      replay(windowId);
    }
    emitCount = 0;
  }
  protected void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      Map<KafkaPartition, KafkaPair<Long, Integer>> recoveredData = (Map<KafkaPartition, KafkaPair<Long, Integer>>) idempotentStorageManager.load(operatorId, windowId);
      if (recoveredData == null) {
        return;
      }

      Map<String, List<PartitionMetadata>> pms = KafkaMetadataUtil.getPartitionsForTopic(getConsumer().brokers, getConsumer().topic);
      if (pms == null) {
       return;
      }

      SimpleKafkaConsumer cons = (SimpleKafkaConsumer)getConsumer();
      // add all partition request in one Fretch request together
      FetchRequestBuilder frb = new FetchRequestBuilder().clientId(cons.getClientId());
      for (Map.Entry<KafkaPartition, KafkaPair<Long, Integer>> rc: recoveredData.entrySet()) {
        KafkaPartition kp = rc.getKey();
        List<PartitionMetadata> pmsVal = pms.get(kp.getClusterId());

        Iterator<PartitionMetadata> pmIterator = pmsVal.iterator();
        PartitionMetadata pm = pmIterator.next();
        while (pm.partitionId() != kp.getPartitionId()) {
          if (!pmIterator.hasNext())
            break;
          pm = pmIterator.next();
        }
        if (pm.partitionId() != kp.getPartitionId())
          continue;

        Broker bk = pm.leader();

        frb.addFetch(consumer.topic, rc.getKey().getPartitionId(), rc.getValue().first, cons.getBufferSize());
        FetchRequest req = frb.build();

        SimpleConsumer ksc = new SimpleConsumer(bk.host(), bk.port(), cons.getTimeout(), cons.getBufferSize(), cons.getClientId());
        FetchResponse fetchResponse = ksc.fetch(req);
        Integer count = 0;
        for (MessageAndOffset msg : fetchResponse.messageSet(consumer.topic, kp.getPartitionId())) {
          emitTuple(msg.message());
          offsetStats.put(kp, msg.offset());
          count = count + 1;
          if (count.equals(rc.getValue().second))
            break;
        }
      }
      if(windowId == idempotentStorageManager.getLargestRecoveryWindow()) {
        // Set the offset positions to the consumer
        Map<KafkaPartition, Long> currentOffsets = new HashMap<KafkaPartition, Long>(cons.getCurrentOffsets());
        // Increment the offsets
        for (Map.Entry<KafkaPartition, Long> e: offsetStats.entrySet()) {
          currentOffsets.put(e.getKey(), e.getValue() + 1);
        }

        cons.resetOffset(currentOffsets);
        cons.start();
      }
    }

    catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
  }
  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    if (currentWindowId > idempotentStorageManager.getLargestRecoveryWindow()) {
      try {
        idempotentStorageManager.save(currentWindowRecoveryState, operatorId, currentWindowId);
      }
      catch (IOException e) {
        throw new RuntimeException("saving recovery", e);
      }
    }
    currentWindowRecoveryState.clear();
  }

  @Override
  public void checkpointed(long windowId)
  {
    // commit the kafka consumer offset
    getConsumer().commitOffset();
  }

  @Override
  public void committed(long windowId)
  {
    try {
      idempotentStorageManager.deleteUpTo(operatorId, windowId);
    }
    catch (IOException e) {
      throw new RuntimeException("deleting state", e);
    }
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void activate(OperatorContext ctx)
  {
    if (context.getValue(OperatorContext.ACTIVATION_WINDOW_ID) != Stateless.WINDOW_ID && context.getValue(OperatorContext.ACTIVATION_WINDOW_ID) < idempotentStorageManager.getLargestRecoveryWindow()) {
      // If it is a replay state, don't start the consumer
      return;
    }
    // Don't start thread here!
    // # of kafka_consumer_threads depends on the type of kafka client and the message
    // metadata(topic/partition/replica) layout
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
    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      return;
    }
    int count = consumer.messageSize();
    if (maxTuplesPerWindow > 0) {
      count = Math.min(count, maxTuplesPerWindow - emitCount);
    }
    for (int i = 0; i < count; i++) {
      KafkaConsumer.KafkaMessage message = consumer.pollMessage();
      // Ignore the duplicate messages
      if(offsetStats.containsKey(message.kafkaPart) && message.offSet <= offsetStats.get(message.kafkaPart))
        continue;
      emitTuple(message.msg);
      offsetStats.put(message.kafkaPart, message.offSet);
      if(!currentWindowRecoveryState.containsKey(message.kafkaPart))
      {
        currentWindowRecoveryState.put(message.kafkaPart, new KafkaPair<Long, Integer>(message.offSet, 1));
      } else {
        Pair<Long, Integer> second = currentWindowRecoveryState.get(message.kafkaPart);
        Integer noOfMessages = second.getSecond();
        currentWindowRecoveryState.put(message.kafkaPart, new KafkaPair<Long, Integer>(second.getFirst(), noOfMessages+1));
      }
    }
    emitCount += count;
  }

  public void setConsumer(K consumer)
  {
    this.consumer = consumer;
  }

  public KafkaConsumer getConsumer()
  {
    return consumer;
  }

  // add topic as operator property
  public void setTopic(String topic)
  {
    this.consumer.setTopic(topic);
  }

  /**
   * Set the zookeeper of the kafka cluster(s) you want to consume data frome
   * Dev should have no worry about of using Simple consumer/High level consumer
   * The operator will discover the brokers that it needs to consume messages from
   */
  public void setZookeeper(String zookeeperString)
  {
    SetMultimap<String, String> theClusters = HashMultimap.create();
    for (String zk : zookeeperString.split(",")) {
      String[] parts = zk.split(":");
      if (parts.length == 3) {
        theClusters.put(parts[0], parts[1] + ":" + parts[2]);
      } else if (parts.length == 2) {
        theClusters.put(KafkaPartition.DEFAULT_CLUSTERID, parts[0] + ":" + parts[1]);
      } else
        throw new IllegalArgumentException("Wrong zookeeper string: " + zookeeperString + "\n"
            + " Expected format should be cluster1:zookeeper1:port1,cluster2:zookeeper2:port2 or zookeeper1:port1,zookeeper:port2");
    }
    this.consumer.setZookeeper(theClusters);
  }

  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return idempotentStorageManager;
  }

  public void setIdempotentStorageManager(IdempotentStorageManager idempotentStorageManager)
  {
    this.idempotentStorageManager = idempotentStorageManager;
  }
}
