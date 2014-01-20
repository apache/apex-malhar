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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class KafkaInputOperatorTest extends KafkaOperatorTestBase
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaInputOperatorTest.class);
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();
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
    ArrayList<T> list;
    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super();
      this.id = id;
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
      list.add(tuple);
      tupleCount.incrementAndGet();
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collections.put(id, list = new ArrayList<T>());
      }
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
    if (isValid) {
      Set<String> brokerSet = new HashSet<String>();
      brokerSet.add("localhost:9092");
      consumer.setBrokerSet(brokerSet);
    }
    node.setConsumer(consumer);
    
    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();
    
    // Wait 30s for consumer finish consuming all the messages
    Assert.assertTrue("TIMEOUT: 30s ", latch.await(30000, TimeUnit.MILLISECONDS));
    
    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", totalCount, collections.get(collector.inputPort.id).size());
    logger.debug(String.format("Number of emitted tuples: %d", collections.get(collector.inputPort.id).size()));
    
    p.close();
    lc.shutdown();
  }

  @Test
  public void testKafkaInputOperator_Highleverl() throws Exception
  {
    int totalCount = 10000;
    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT);
    props.put("group.id", "group1");
    props.put("consumer.id", "default_consumer");
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
    collections.clear();
    super.afterTest();
  }
}
