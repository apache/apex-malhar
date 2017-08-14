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
package org.apache.apex.benchmark.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Partitioner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * This operator keep sending constant messages(1kb each) in {@link #threadNum} threads.&nbsp;
 * Messages are distributed evenly to partitions.
 * <p></p>
 *
 * @displayName Benchmark Partitionable Kafka Output
 * @category Messaging
 * @tags output operator
 * @since 0.9.3
 */
public class BenchmarkPartitionableKafkaOutputOperator implements
    Partitioner<BenchmarkPartitionableKafkaOutputOperator>, InputOperator, ActivationListener<OperatorContext>
{

  private static final Logger logger = LoggerFactory.getLogger(BenchmarkPartitionableKafkaOutputOperator.class);

  private String topic = "benchmark";

  @Min(1)
  private int partitionCount = 5;

  private String brokerList = "localhost:9092";

  private int threadNum = 1;

  //define constant message
  private byte[] constantMsg = null;

  private int msgSize = 1024;

  private transient ScheduledExecutorService ses = Executors.newScheduledThreadPool(5);

  private final boolean controlThroughput = true;

  private int msgsSecThread = 1000;

  private int stickyKey = 0;

  private transient Runnable r = new Runnable()
  {

    Producer<String, String> producer = null;

    @Override
    public void run()
    {
      logger.info("Start produce data .... ");
      Properties props = new Properties();
      props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
      props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
      props.put("metadata.broker.list", brokerList);
//      props.put("metadata.broker.list", "localhost:9092");
      props.setProperty("partitioner.class", KafkaTestPartitioner.class.getCanonicalName());
      props.setProperty("producer.type", "async");
//      props.setProperty("send.buffer.bytes", "1048576");
      props.setProperty("topic.metadata.refresh.interval.ms", "10000");

      if (producer == null) {
        producer = new Producer<String, String>(new ProducerConfig(props));
      }
      long k = 0;

      while (k < msgsSecThread || !controlThroughput) {
        long key = (stickyKey >= 0 ? stickyKey : k);
        k++;
        producer.send(new KeyedMessage<String, String>(topic, "" + key, new String(constantMsg)));
        if (k == Long.MAX_VALUE) {
          k = 0;
        }
      }
    }
  };

  @Override
  public void beginWindow(long arg0)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(OperatorContext arg0)
  {

  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void emitTuples()
  {

  }

  @Override
  public void partitioned(Map<Integer, Partition<BenchmarkPartitionableKafkaOutputOperator>> partitions)
  {
  }

  /**
   * <b>Note:</b> This partitioner does not support parallel partitioning.<br/><br/>
   * {@inheritDoc}
   */
  @Override
  public Collection<Partition<BenchmarkPartitionableKafkaOutputOperator>> definePartitions(
      Collection<Partition<BenchmarkPartitionableKafkaOutputOperator>> partitions, PartitioningContext context)
  {

    ArrayList<Partition<BenchmarkPartitionableKafkaOutputOperator>> newPartitions =
        new ArrayList<Partitioner.Partition<BenchmarkPartitionableKafkaOutputOperator>>(partitionCount);

    for (int i = 0; i < partitionCount; i++) {
      BenchmarkPartitionableKafkaOutputOperator bpkoo = new BenchmarkPartitionableKafkaOutputOperator();
      bpkoo.setPartitionCount(partitionCount);
      bpkoo.setTopic(topic);
      bpkoo.setBrokerList(brokerList);
      bpkoo.setStickyKey(i);
      Partition<BenchmarkPartitionableKafkaOutputOperator> p =
          new DefaultPartition<BenchmarkPartitionableKafkaOutputOperator>(bpkoo);
      newPartitions.add(p);
    }
    return newPartitions;
  }

  @Override
  public void activate(OperatorContext arg0)
  {

    logger.info("Activate the benchmark kafka output operator .... ");
    constantMsg = new byte[msgSize];
    for (int i = 0; i < constantMsg.length; i++) {
      constantMsg[i] = (byte)('a' + i % 26);
    }

    for (int i = 0; i < threadNum; i++) {
      if (controlThroughput) {
        ses.scheduleAtFixedRate(r, 0, 1, TimeUnit.SECONDS);
      } else {
        ses.submit(r);
      }
    }

  }

  @Override
  public void deactivate()
  {

  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public int getPartitionCount()
  {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  public String getBrokerList()
  {
    return brokerList;
  }

  public void setBrokerList(String brokerList)
  {
    this.brokerList = brokerList;
  }

  public int getThreadNum()
  {
    return threadNum;
  }

  public void setThreadNum(int threadNum)
  {
    this.threadNum = threadNum;
  }

  public void setMsgSize(int msgSize)
  {
    this.msgSize = msgSize;
  }

  public int getMsgSize()
  {
    return msgSize;
  }

  public void setMsgsSecThread(int msgsSecThread)
  {
    this.msgsSecThread = msgsSecThread;
  }

  public int getMsgsSecThread()
  {
    return msgsSecThread;
  }

  public int getStickyKey()
  {
    return stickyKey;
  }

  public void setStickyKey(int stickyKey)
  {
    this.stickyKey = stickyKey;
  }

}
