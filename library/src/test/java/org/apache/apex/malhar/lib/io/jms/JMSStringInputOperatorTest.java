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
package org.apache.apex.malhar.lib.io.jms;

import java.io.File;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Tests for {@link JMSStringInputOperator}
 */
public class JMSStringInputOperatorTest
{
  public static class TestMeta extends TestWatcher
  {
    String baseDir;
    JMSStringInputOperator operator;
    CollectorTestSink<Object> sink;
    Context.OperatorContext context;
    JMSTestBase testBase;

    @Override
    protected void starting(Description description)
    {
      testBase = new JMSTestBase();
      try {
        testBase.beforTest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      String methodName = description.getMethodName();
      String className = description.getClassName();
      baseDir = "target/" + className + "/" + methodName;

      Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
      attributeMap.put(Context.OperatorContext.SPIN_MILLIS, 500);
      attributeMap.put(Context.DAGContext.APPLICATION_PATH, baseDir);

      context = mockOperatorContext(1, attributeMap);
      operator = new JMSStringInputOperator();
      operator.setSubject("TEST.FOO");
      operator.getConnectionFactoryProperties().put(JMSTestBase.AMQ_BROKER_URL, "vm://localhost");

      sink = new CollectorTestSink<>();
      operator.output.setSink(sink);
      operator.setup(context);
      operator.activate(context);
    }

    @Override
    protected void finished(Description description)
    {
      operator.deactivate();
      operator.teardown();
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
        testBase.afterTest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testStringMsgInput() throws Exception
  {
    produceMsg(10);
    Thread.sleep(1000);
    testMeta.operator.emitTuples();
    Assert.assertEquals("num of messages", 10, testMeta.sink.collectedTuples.size());
  }

  @Test
  public void testRecoveryAndIdempotency() throws Exception
  {
    produceMsg(100);
    Thread.sleep(1000);
    testMeta.operator.beginWindow(1);
    testMeta.operator.emitTuples();
    testMeta.operator.endWindow();

    //failure and then re-deployment of operator
    testMeta.sink.collectedTuples.clear();
    testMeta.operator.setup(testMeta.context);
    testMeta.operator.activate(testMeta.context);

    Assert.assertEquals("largest recovery window", 1,
        testMeta.operator.getWindowDataManager().getLargestCompletedWindow());

    testMeta.operator.beginWindow(1);
    testMeta.operator.endWindow();
    Assert.assertEquals("num of messages in window 1", 100, testMeta.sink.collectedTuples.size());
    testMeta.sink.collectedTuples.clear();
  }

  private void produceMsg(int numMessages) throws Exception
  {
    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

    // Create a Connection
    Connection connection = connectionFactory.createConnection();
    connection.start();

    // Create a Session
    Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

    // Create the destination (Topic or Queue)
    Destination destination = session.createQueue("TEST.FOO");

    // Create a MessageProducer from the Session to the Topic or Queue
    MessageProducer producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

    // Create a messages
    String text = "Hello world! From tester producer";
    TextMessage message = session.createTextMessage(text);
    for (int i = 0; i < numMessages; i++) {
      producer.send(message);
    }

    // Clean up
    session.close();
    connection.close();

  }

  private static final transient Logger LOG = LoggerFactory.getLogger(JMSStringInputOperatorTest.class);
}
