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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaOutputOperatorTest extends KafkaOperatorTestBase
{
  String testName;
  private static List<Person> tupleCollection = new LinkedList<>();
  private final String VALUE_DESERIALIZER = "org.apache.apex.malhar.kafka.KafkaHelper";
  private final String VALUE_SERIALIZER = "org.apache.apex.malhar.kafka.KafkaHelper";

  public static String APPLICATION_PATH = baseDir + File.separator + "MyKafkaApp" + File.separator;

  @Before
  public void before()
  {
    FileUtils.deleteQuietly(new File(APPLICATION_PATH));
    testName = TEST_TOPIC + testCounter++;
    createTopic(0, testName);
    if (hasMultiCluster) {
      createTopic(1, testName);
    }
  }

  @After
  public void after()
  {
    FileUtils.deleteQuietly(new File(APPLICATION_PATH));
  }

  @Test
  public void testExactlyOnceWithFailure() throws Exception
  {
    List<Person> toKafka = GenerateList();

    sendDataToKafka(true, toKafka, true, false);

    List<Person> fromKafka = ReadFromKafka();

    Assert.assertTrue("With Failure", compare(fromKafka, toKafka));
  }

  @Test
  public void testExactlyOnceWithNoFailure() throws Exception
  {
    List<Person> toKafka = GenerateList();

    sendDataToKafka(true, toKafka, false, false);

    List<Person> fromKafka = ReadFromKafka();

    Assert.assertTrue("With No Failure", compare(fromKafka, toKafka));
  }

  @Test
  public void testExactlyOnceWithDifferentTuplesAfterRecovery() throws Exception
  {
    List<Person> toKafka = GenerateList();

    try {
      sendDataToKafka(true, toKafka, true, true);
    } catch (RuntimeException ex) {

      boolean expectedException = false;
      if (ex.getMessage().contains("Violates")) {
        expectedException = true;
      }

      Assert.assertTrue("Different tuples after recovery", expectedException);
      return;
    }

    Assert.assertTrue("Wrong tuples during replay, should throw exception", false);
  }

  @Test
  public void testKafkaOutput() throws Exception
  {
    List<Person> toKafka = GenerateList();

    sendDataToKafka(false, toKafka, false, false);

    List<Person> fromKafka = ReadFromKafka();

    Assert.assertTrue("No failure", compare(fromKafka, toKafka));
  }

  @Test
  public void testKafkaOutputWithFailure() throws Exception
  {
    List<Person> toKafka = GenerateList();

    sendDataToKafka(false, toKafka, true, true);

    List<Person> fromKafka = ReadFromKafka();

    Assert.assertTrue("No failure", fromKafka.size() > toKafka.size());
  }

  private void sendDataToKafka(boolean exactlyOnce, List<Person> toKafka, boolean hasFailure,
      boolean differentTuplesAfterRecovery) throws InterruptedException
  {
    Properties props = new Properties();
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);
    if (!exactlyOnce) {
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaSinglePortExactlyOnceOutputOperator.KEY_SERIALIZER);
    }
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getClusterConfig());
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);

    Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(Context.DAGContext.APPLICATION_NAME, "MyKafkaApp");
    attributeMap.put(DAG.APPLICATION_PATH, APPLICATION_PATH);

    OperatorContext operatorContext = mockOperatorContext(2, attributeMap);

    cleanUp(operatorContext);

    Operator kafkaOutput;
    DefaultInputPort<Person> inputPort;

    if (exactlyOnce) {
      KafkaSinglePortExactlyOnceOutputOperator<Person> kafkaOutputTemp =
          ResetKafkaOutput(testName, props, operatorContext);
      inputPort = kafkaOutputTemp.inputPort;
      kafkaOutput = kafkaOutputTemp;
    } else {
      KafkaSinglePortOutputOperator<String, Person> kafkaOutputTemp =
          ResetKafkaSimpleOutput(testName, props, operatorContext);
      inputPort = kafkaOutputTemp.inputPort;
      kafkaOutput = kafkaOutputTemp;
    }

    kafkaOutput.beginWindow(1);
    inputPort.getSink().put(toKafka.get(0));
    inputPort.getSink().put(toKafka.get(1));
    inputPort.getSink().put(toKafka.get(2));
    kafkaOutput.endWindow();
    kafkaOutput.beginWindow(2);
    inputPort.getSink().put(toKafka.get(3));
    inputPort.getSink().put(toKafka.get(4));
    inputPort.getSink().put(toKafka.get(5));
    kafkaOutput.endWindow();
    kafkaOutput.beginWindow(3);
    inputPort.getSink().put(toKafka.get(6));
    inputPort.getSink().put(toKafka.get(7));

    if (hasFailure) {
      if (exactlyOnce) {
        KafkaSinglePortExactlyOnceOutputOperator<Person> kafkaOutputTemp =
            ResetKafkaOutput(testName, props, operatorContext);
        inputPort = kafkaOutputTemp.inputPort;
        kafkaOutput = kafkaOutputTemp;
      } else {
        KafkaSinglePortOutputOperator<String, Person> kafkaOutputTemp =
            ResetKafkaSimpleOutput(testName, props, operatorContext);
        inputPort = kafkaOutputTemp.inputPort;
        kafkaOutput = kafkaOutputTemp;
      }

      kafkaOutput.beginWindow(2);
      inputPort.getSink().put(toKafka.get(3));
      inputPort.getSink().put(toKafka.get(4));
      inputPort.getSink().put(toKafka.get(5));
      kafkaOutput.endWindow();
      kafkaOutput.beginWindow(3);
      inputPort.getSink().put(toKafka.get(6));

      if (!differentTuplesAfterRecovery) {
        inputPort.getSink().put(toKafka.get(7));
      }
    }

    inputPort.getSink().put(toKafka.get(8));
    inputPort.getSink().put(toKafka.get(9));
    kafkaOutput.endWindow();
    kafkaOutput.beginWindow(4);
    inputPort.getSink().put(toKafka.get(10));
    inputPort.getSink().put(toKafka.get(11));
    kafkaOutput.endWindow();

    cleanUp(operatorContext);
  }

  private KafkaSinglePortExactlyOnceOutputOperator<Person> ResetKafkaOutput(
      String testName, Properties props, Context.OperatorContext operatorContext)
  {
    KafkaSinglePortExactlyOnceOutputOperator<Person> kafkaOutput = new KafkaSinglePortExactlyOnceOutputOperator<>();
    kafkaOutput.setTopic(testName);
    kafkaOutput.setProperties(props);
    kafkaOutput.setup(operatorContext);

    return kafkaOutput;
  }

  private KafkaSinglePortOutputOperator<String, Person> ResetKafkaSimpleOutput(
      String testName, Properties props, Context.OperatorContext operatorContext)
  {
    KafkaSinglePortOutputOperator<String, Person> kafkaOutput = new KafkaSinglePortOutputOperator<>();
    kafkaOutput.setTopic(testName);
    kafkaOutput.setProperties(props);
    kafkaOutput.setup(operatorContext);

    return kafkaOutput;
  }

  private void cleanUp(Context.OperatorContext operatorContext)
  {
    FSWindowDataManager windowDataManager = new FSWindowDataManager();
    windowDataManager.setup(operatorContext);
    try {
      windowDataManager.committed(windowDataManager.getLargestCompletedWindow());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private boolean compare(List<Person> fromKafka, List<Person> toKafka)
  {
    if (fromKafka.size() != toKafka.size()) {
      return false;
    }

    for (int i = 0; i < fromKafka.size(); ++i) {
      if (!fromKafka.get(i).equals(toKafka.get(i))) {
        return false;
      }
    }

    return true;
  }

  private String getClusterConfig()
  {
    String l = "localhost:";
    return l + TEST_KAFKA_BROKER_PORT[0] +
      (hasMultiCluster ? ";" + l + TEST_KAFKA_BROKER_PORT[1] : "");
  }

  private List<Person> GenerateList()
  {
    List<Person> people = new ArrayList<>();

    for (Integer i = 0; i < 12; ++i) {
      people.add(new Person(i.toString(), i));
    }

    return people;
  }

  private List<Person> ReadFromKafka()
  {
    tupleCollection.clear();

    // Create KafkaSinglePortStringInputOperator
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS_CONFIG, getClusterConfig());
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, KafkaSinglePortExactlyOnceOutputOperator.KEY_DESERIALIZER);
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
    props.put(GROUP_ID_CONFIG, "KafkaTest");

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KafkaSinglePortStringInputOperator
    KafkaSinglePortInputOperator node = dag.addOperator("Kafka input", KafkaSinglePortInputOperator.class);
    node.setConsumerProps(props);
    node.setInitialPartitionCount(1);
    // set topic
    node.setTopics(testName);
    node.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
    node.setClusters(getClusterConfig());
    node.setStrategy("one_to_one");

    // Create Test tuple collector
    CollectorModule collector1 = dag.addOperator("collector", new CollectorModule());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector1.inputPort);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.run(30000);

    return tupleCollection;
  }

  public static class CollectorModule extends BaseOperator
  {
    public final transient CollectorInputPort inputPort = new CollectorInputPort(this);

    long currentWindowId;
    long operatorId;

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      operatorId = context.getId();
    }

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      currentWindowId = windowId;
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
    }
  }

  public static class CollectorInputPort extends DefaultInputPort<byte[]>
  {
    CollectorModule ownerNode;

    CollectorInputPort(CollectorModule node)
    {
      this.ownerNode = node;
    }

    @Override
    public void process(byte[] bt)
    {
      tupleCollection.add(new KafkaHelper().deserialize("r", bt));
    }
  }

  public static class Person
  {
    public String name;
    public Integer age;

    public Person(String name, Integer age)
    {
      this.name = name;
      this.age = age;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Person person = (Person)o;

      if (name != null ? !name.equals(person.name) : person.name != null) {
        return false;
      }

      return age != null ? age.equals(person.age) : person.age == null;
    }

    @Override
    public int hashCode()
    {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (age != null ? age.hashCode() : 0);
      return result;
    }

    @Override
    public String toString()
    {
      return name + age.toString();
    }
  }
}
