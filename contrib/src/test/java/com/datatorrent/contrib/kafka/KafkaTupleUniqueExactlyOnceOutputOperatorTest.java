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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.api.OperatorDeployInfo;

import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;

public class KafkaTupleUniqueExactlyOnceOutputOperatorTest extends KafkaOperatorTestBase
{
  public static final int TUPLE_NUM_IN_ONE_WINDOW = 10;
  public static final String topic1 = "OperatorTest1";
  public static final String controlTopic1 = "ControlTopic1";

  public static final String topic2 = "OperatorTest2";
  public static final String controlTopic2 = "ControlTopic2";

  public static final String topic3 = "OperatorTest3";
  public static final String controlTopic3 = "ControlTopic3";

  public static class TupleUniqueExactlyOnceKafkaOutputTestOperator
      extends AbstractTupleUniqueExactlyOnceKafkaOutputOperator<Integer, String, String>
  {
    protected transient StringDecoder decoder = null;

    @Override
    public void setup(OperatorContext context)
    {
      decoder = new StringDecoder(null);
      super.setup(context);
    }
    
    @Override
    protected Pair<String, String> tupleToKeyValue(Integer tuple)
    {
      return new Pair<>(String.valueOf(tuple % 2), String.valueOf(tuple));
    }

    @Override
    protected <T> boolean equals(byte[] bytes, T value)
    {
      if (bytes == null && value == null) {
        return true;
      }
      if (value == null) {
        return false;
      }
      return value.equals(decoder.fromBytes(bytes));
    }

  }

  protected void createTopic(String topicName)
  {
    createTopic(0, topicName);
    if (hasMultiCluster) {
      createTopic(1, topicName);
    }
  }

  protected ProducerConfig createKafkaControlProducerConfig()
  {
    return new ProducerConfig(this.getKafkaProperties());
  }

  /**
   * This test case there are only one operator partition, and the order of data
   * changed when recovery.
   */
  @Test
  public void testOutOfOrder()
  {
    OperatorDeployInfo context = new OperatorDeployInfo();
    context.id = 1;
    int[] expectedTuple = new int[(int)(TUPLE_NUM_IN_ONE_WINDOW * 3)];
    int tupleIndex = 0;
    long windowId = 0;
    {
      //create required topics
      createTopic(topic1);
      createTopic(controlTopic1);

      TupleUniqueExactlyOnceKafkaOutputTestOperator operator = createOperator(topic1, controlTopic1, 1);

      int i = 0;
      for (int windowCount = 0; windowCount < 2; ++windowCount) {
        operator.beginWindow(windowId++);

        for (; i < TUPLE_NUM_IN_ONE_WINDOW * (windowCount + 1); ++i) {
          operator.processTuple(i);
          expectedTuple[tupleIndex++] = i;
        }
        waitMills(500);
        operator.endWindow();
      }

      //last window, the crash window
      operator.beginWindow(windowId++);
      for (; i < TUPLE_NUM_IN_ONE_WINDOW * 2.5; ++i) {
        operator.processTuple(i);
        expectedTuple[tupleIndex++] = i;
      }

      //crashed now.
    }

    //let kafka message send to server
    waitMills(1000);

    {
      //recovery
      TupleUniqueExactlyOnceKafkaOutputTestOperator operator = new TupleUniqueExactlyOnceKafkaOutputTestOperator();
      operator.setTopic(topic1);
      operator.setControlTopic(controlTopic1);
      operator.setConfigProperties(getKafkaProperties());

      operator.setup(context);

      //assume replay start with 2nd window, but different order
      int i = TUPLE_NUM_IN_ONE_WINDOW;

      windowId = 1;
      operator.beginWindow(windowId++);
      for (; i < TUPLE_NUM_IN_ONE_WINDOW * 2; i += 2) {
        operator.processTuple(i);
      }
      i = TUPLE_NUM_IN_ONE_WINDOW + 1;
      for (; i < TUPLE_NUM_IN_ONE_WINDOW * 2; i += 2) {
        operator.processTuple(i);
      }
      waitMills(500);
      operator.endWindow();

      //3rd window, in different order
      operator.beginWindow(windowId++);
      i = TUPLE_NUM_IN_ONE_WINDOW * 2;
      for (; i < TUPLE_NUM_IN_ONE_WINDOW * 3; i += 2) {
        operator.processTuple(i);
        if (i >= TUPLE_NUM_IN_ONE_WINDOW * 2.5) {
          expectedTuple[tupleIndex++] = i;
        }
      }

      i = TUPLE_NUM_IN_ONE_WINDOW * 2 + 1;
      for (; i < TUPLE_NUM_IN_ONE_WINDOW * 3; i += 2) {
        operator.processTuple(i);
        if (i >= TUPLE_NUM_IN_ONE_WINDOW * 2.5) {
          expectedTuple[tupleIndex++] = i;
        }
      }
    }

    int[] actualTuples = readTuplesFromKafka(topic1);
    Assert.assertArrayEquals(expectedTuple, actualTuples);
  }

  protected TupleUniqueExactlyOnceKafkaOutputTestOperator createOperator(String topic, String controlTopic, int id)
  {
    TupleUniqueExactlyOnceKafkaOutputTestOperator operator = new TupleUniqueExactlyOnceKafkaOutputTestOperator();
    operator.setTopic(topic);
    operator.setControlTopic(controlTopic);

    operator.setConfigProperties(getKafkaProperties());
    OperatorDeployInfo context = new OperatorDeployInfo();
    context.id = id;
    operator.setup(context);

    return operator;
  }

  /**
   * This test case test the case the tuple go to other operator partition when
   * recovery.
   */
  @Test
  public void testDifferentPartition()
  {
    //hasMultiPartition = true;

    int[] expectedTuples = new int[(int)(TUPLE_NUM_IN_ONE_WINDOW * 6)];
    int tupleIndex = 0;
    long windowId1 = 0;
    long windowId2 = 0;

    //create required topics
    createTopic(topic2);
    createTopic(controlTopic2);

    {
      TupleUniqueExactlyOnceKafkaOutputTestOperator operator1 = createOperator(topic2, controlTopic2, 1);
      TupleUniqueExactlyOnceKafkaOutputTestOperator operator2 = createOperator(topic2, controlTopic2, 2);
      TupleUniqueExactlyOnceKafkaOutputTestOperator[] operators = new TupleUniqueExactlyOnceKafkaOutputTestOperator[] {
          operator1, operator2 };

      //send as round robin
      int i = 0;
      for (int windowCount = 0; windowCount < 2; ++windowCount) {
        operator1.beginWindow(windowId1++);
        operator2.beginWindow(windowId2++);

        for (; i < TUPLE_NUM_IN_ONE_WINDOW * (windowCount + 1) * 2; ++i) {
          operators[i % 2].processTuple(i);
          expectedTuples[tupleIndex++] = i;
        }
        waitMills(500);
        operator1.endWindow();
        operator2.endWindow();
      }

      //last window, the crash window
      operator1.beginWindow(windowId1++);
      operator2.beginWindow(windowId2++);
      for (; i < TUPLE_NUM_IN_ONE_WINDOW * 2.5 * 2; ++i) {
        operators[i % 2].processTuple(i);
        expectedTuples[tupleIndex++] = i;
      }

      //crashed now.
    }

    //let kafka message send to server
    waitMills(1000);
    int lastTuple = tupleIndex - 1;
    {
      //recovery
      TupleUniqueExactlyOnceKafkaOutputTestOperator operator1 = createOperator(topic2, controlTopic2, 1);
      TupleUniqueExactlyOnceKafkaOutputTestOperator operator2 = createOperator(topic2, controlTopic2, 2);
      //tuple go to different partition
      TupleUniqueExactlyOnceKafkaOutputTestOperator[] operators = new TupleUniqueExactlyOnceKafkaOutputTestOperator[] {
          operator2, operator1 };

      //assume replay start with 2nd window, but different order
      int i = TUPLE_NUM_IN_ONE_WINDOW * 2;

      windowId1 = 1;
      windowId2 = 1;

      //window id: 1, 2
      for (int windowCount = 0; windowCount < 2; ++windowCount) {
        operator1.beginWindow(windowId1++);
        operator2.beginWindow(windowId2++);

        for (; i < TUPLE_NUM_IN_ONE_WINDOW * (windowCount + 2) * 2; ++i) {
          operators[i % 2].processTuple(i);
          if (i > lastTuple) {
            expectedTuples[tupleIndex++] = i;
          }
        }
        waitMills(500);
        operator1.endWindow();
        operator2.endWindow();
      }
    }

    int[] actualTuples = readTuplesFromKafka(topic2);
    Arrays.sort(actualTuples);
    Arrays.sort(expectedTuples);

    assertArrayEqualsWithDetailInfo(expectedTuples, actualTuples);
  }

  /**
   * This test case test only one operator partition crash, while the other
   * operator partition keep on write data to the same Kafka partition.
   */
  @Test
  public void testOnePartitionCrash()
  {

    int[] expectedTuples = new int[(int)(TUPLE_NUM_IN_ONE_WINDOW * 6)];
    int tupleIndex = 0;
    long windowId1 = 0;
    long windowId2 = 0;

    //create required topics
    createTopic(topic3);
    createTopic(controlTopic3);

    {
      TupleUniqueExactlyOnceKafkaOutputTestOperator operator1 = createOperator(topic3, controlTopic3, 1);
      TupleUniqueExactlyOnceKafkaOutputTestOperator operator2 = createOperator(topic3, controlTopic3, 2);
      TupleUniqueExactlyOnceKafkaOutputTestOperator[] operators = new TupleUniqueExactlyOnceKafkaOutputTestOperator[] {
          operator1, operator2 };

      //send as round robin
      int i = 0;
      for (int windowCount = 0; windowCount < 2; ++windowCount) {
        operator1.beginWindow(windowId1++);
        operator2.beginWindow(windowId2++);

        for (; i < TUPLE_NUM_IN_ONE_WINDOW * (windowCount + 1) * 2; ++i) {
          operators[i % 2].processTuple(i);
          expectedTuples[tupleIndex++] = i;
        }
        waitMills(500);
        operator1.endWindow();
        operator2.endWindow();
      }

      //operator1 crash, while operator2 alive
      operator1.beginWindow(windowId1++);
      //operator1 handle even number;
      for (; i < TUPLE_NUM_IN_ONE_WINDOW * 2.5 * 2; i += 2) {
        operators[i % 2].processTuple(i);
        expectedTuples[tupleIndex++] = i;
      }

      //operator1 crashed now.

      //operator2 still alive, operator2 handle odd number
      operator2.beginWindow(windowId2++);
      i = TUPLE_NUM_IN_ONE_WINDOW * 4 + 1;
      for (; i < TUPLE_NUM_IN_ONE_WINDOW * 3 * 2; i += 2) {
        operator2.processTuple(i);
        expectedTuples[tupleIndex++] = i;
      }
      operator2.endWindow();

    }

    //let kafka message send to server
    waitMills(1000);

    //operator1 recover from second window
    int lastTuple = (int)(TUPLE_NUM_IN_ONE_WINDOW * 2.5 * 2) - 1;
    {
      //recovery
      TupleUniqueExactlyOnceKafkaOutputTestOperator operator1 = createOperator(topic3, controlTopic3, 1);

      //assume replay start with 2nd window, same order
      int i = TUPLE_NUM_IN_ONE_WINDOW * 2;

      windowId1 = 1;

      //window id: 1, 2
      for (int windowCount = 0; windowCount < 2; ++windowCount) {
        operator1.beginWindow(windowId1++);

        for (; i < TUPLE_NUM_IN_ONE_WINDOW * (windowCount + 2) * 2; i += 2) {
          operator1.processTuple(i);
          if (i > lastTuple) {
            expectedTuples[tupleIndex++] = i;
          }
        }
        waitMills(500);
        operator1.endWindow();
      }
    }

    int[] actualTuples = readTuplesFromKafka(topic3);
    Arrays.sort(actualTuples);
    Arrays.sort(expectedTuples);

    assertArrayEqualsWithDetailInfo(expectedTuples, actualTuples);
  }

  /**
   * Test the application which using TupleUniqueExactlyOnceKafkaOutputTestOperator is launchalbe in local mode
   */
  @Test
  public void testLaunchApp() throws Exception
  {
    Configuration conf = new Configuration(false);
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    TupleGenerateOperator generateOperator = new TupleGenerateOperator();
    dag.addOperator("GenerateOperator", generateOperator);
    
    TupleUniqueExactlyOnceKafkaOutputTestOperator testOperator = new TupleUniqueExactlyOnceKafkaOutputTestOperator();
    dag.addOperator("TestOperator", testOperator);

    dag.addStream("stream", generateOperator.outputPort, testOperator.inputPort);
    
    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(5000);

    lc.shutdown();
  }

  public static void assertArrayEqualsWithDetailInfo(int[] expectedTuples, int[] actualTuples)
  {
    Assert.assertTrue("Length incorrect. expected " + expectedTuples.length + "; actual " + actualTuples.length,
        actualTuples.length == expectedTuples.length);
    for (int i = 0; i < actualTuples.length; ++i) {
      Assert.assertEquals("Not equal. index=" + i + ", expected=" + expectedTuples[i] + ", actual=" + actualTuples[i],
          actualTuples[i], expectedTuples[i]);
    }
  }

  public void waitMills(long millis)
  {
    try {
      Thread.sleep(millis);
    } catch (Exception e) {
      //ignore
    }
  }

  public int[] readTuplesFromKafka(String topic)
  {
    StringDecoder decoder = new StringDecoder(null);
    Map<Integer, Long> partitionToStartOffset = Maps.newHashMap();
    partitionToStartOffset.put(0, 0L);

    this.waitMills(1000);

    Map<Integer, List<Pair<byte[], byte[]>>> partitionToMessages = Maps.newHashMap();
    KafkaUtil.readMessagesAfterOffsetTo("TestOperator", getBrokerSet(), topic, partitionToStartOffset,
        partitionToMessages);

    List<Pair<byte[], byte[]>> msgList = partitionToMessages.get(0);
    int[] values = new int[msgList.size()];
    int index = 0;
    for (Pair<byte[], byte[]> msg : msgList) {
      values[index++] = Integer.valueOf(decoder.fromBytes(msg.second));
    }
    return values;
  }


  protected Set<String> getBrokerSet()
  {
    return Sets.newHashSet((String)getKafkaProperties().get(KafkaMetadataUtil.PRODUCER_PROP_BROKERLIST));
  }

  public Properties getKafkaProperties()
  {
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    //props.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    //props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", "localhost:9092");
    //props.setProperty("producer.type", "sync");
    props.setProperty("producer.type", "async");
    props.setProperty("queue.buffering.max.ms", "100");
    props.setProperty("queue.buffering.max.messages", "5");
    props.setProperty("batch.num.messages", "5");
    return props;
  }
  
  
  public static class TupleGenerateOperator extends BaseOperator implements InputOperator
  {
    @OutputPortFieldAnnotation(optional = true)
    public final transient DefaultOutputPort<Integer> outputPort = new DefaultOutputPort<>();
    protected int value = 0;

    @Override
    public void emitTuples()
    {
      if (!outputPort.isConnected()) {
        return;
      }

      for (int i = 0; i < 100; ++i) {
        outputPort.emit(++value);
      }
    }
  }
}
