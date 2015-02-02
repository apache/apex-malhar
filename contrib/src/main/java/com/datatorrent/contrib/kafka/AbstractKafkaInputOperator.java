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

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import kafka.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Operator.CheckpointListener;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

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
  @NotNull
  @Valid
  protected KafkaConsumer consumer = new SimpleKafkaConsumer();

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
    logger.debug("consumer {} topic {} cacheSize {}", consumer, consumer.getTopic(), consumer.getCacheSize());
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
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void activate(OperatorContext ctx)
  {
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
    int count = consumer.messageSize();
    if (maxTuplesPerWindow > 0) {
      count = Math.min(count, maxTuplesPerWindow - emitCount);
    }
    for (int i = 0; i < count; i++) {
      emitTuple(consumer.pollMessage());
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
    SetMultimap<String, String> theClusters = HashMultimap.<String, String>create();
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

}
