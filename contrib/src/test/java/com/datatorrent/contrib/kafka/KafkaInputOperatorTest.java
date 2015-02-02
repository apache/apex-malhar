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

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.DAG.Locality;

public class KafkaInputOperatorTest extends KafkaOperatorTestBase
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaInputOperatorTest.class);
  static AtomicInteger tupleCount = new AtomicInteger();
  static CountDownLatch latch;

  /**
   * Test Operator to collect tuples from KafkaSingleInputStringOperator.
   * 
   * @param <T>
   */
  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("myInput", this);
  }

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {

    public CollectorInputPort(String id, Operator module)
    {
      super();
    }

    @Override
    public void process(T tuple)
    {
      if (tuple.equals(KafkaOperatorTestBase.END_TUPLE)) {
        if (latch != null) {
          latch.countDown();
        }
        return;
      }
      tupleCount.incrementAndGet();
    }

    @Override
    public void setConnected(boolean flag)
    {
      tupleCount.set(0);
    }
  }

  /**
   * Test AbstractKafkaSinglePortInputOperator (i.e. an input adapter for
   * Kafka, aka consumer). This module receives data from an outside test
   * generator through Kafka message bus and feed that data into Malhar
   * streaming platform.
   * 
   * [Generate message and send that to Kafka message bus] ==> [Receive that
   * message through Kafka input adapter(i.e. consumer) and send using
   * emitTuples() interface on output port during onMessage call]
   * 
   * 
   * @throws Exception
   */
  public void testKafkaInputOperator(int sleepTime, final int totalCount, KafkaConsumer consumer, boolean isValid) throws Exception
  {
    // initial the latch for this test
    latch = new CountDownLatch(1);
    
    
 // Start producer
    KafkaTestProducer p = new KafkaTestProducer(TEST_TOPIC);
    p.setSendCount(totalCount);
    new Thread(p).start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();



    // Create KafkaSinglePortStringInputOperator
    KafkaSinglePortStringInputOperator node = dag.addOperator("Kafka message consumer", KafkaSinglePortStringInputOperator.class);
    consumer.setTopic(TEST_TOPIC);

    node.setConsumer(consumer);
    
    if (isValid) {
      node.setZookeeper("localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT[0]);
    }
    
    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();
    
    // Wait 30s for consumer finish consuming all the messages
    Assert.assertTrue("TIMEOUT: 30s ", latch.await(300000, TimeUnit.MILLISECONDS));
    
    // Check results
    Assert.assertEquals("Tuple count", totalCount, tupleCount.intValue());
    logger.debug(String.format("Number of emitted tuples: %d", tupleCount.intValue()));
    
    p.close();
    lc.shutdown();
  }

  @Test
  public void testKafkaInputOperator_Highleverl() throws Exception
  {
    int totalCount = 10000;
    Properties props = new Properties();
    props.put("group.id", "group1");
    // This damn property waste me 2 days! It's a 0.8 new property. "smallest" means
    // reset the consumer to the beginning of the message that is not consumed yet
    // otherwise it wont get any of those the produced before!
    KafkaConsumer k = new HighlevelKafkaConsumer(props);
    k.setInitialOffset("earliest");
    testKafkaInputOperator(1000, totalCount, k, true);
  }
  
  @Test
  public void testKafkaInputOperator_Simple() throws Exception
  {
    int totalCount = 10000;
    KafkaConsumer k = new SimpleKafkaConsumer();
    k.setInitialOffset("earliest");
    testKafkaInputOperator(1000, totalCount, k, true);
  }
  
  @Test
  public void testKafkaInputOperator_Invalid() throws Exception
  {
    int totalCount = 10000;
    SimpleKafkaConsumer consumer = new SimpleKafkaConsumer();
    try{
      testKafkaInputOperator(1000, totalCount,consumer, false);
    }catch(Exception e){
      // invalid host setup expect to fail here
      Assert.assertEquals("Error creating local cluster", e.getMessage());
    }
  }

  @Override
  @After
  public void afterTest()
  {
    tupleCount.set(0);
    super.afterTest();
  }
}
