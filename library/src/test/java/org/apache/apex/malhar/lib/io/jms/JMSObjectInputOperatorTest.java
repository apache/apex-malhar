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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.DTThrowable;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class JMSObjectInputOperatorTest
{

  public static class TestMeta extends TestWatcher
  {
    String baseDir;
    JMSObjectInputOperator operator;
    CollectorTestSink<Object> sink;
    OperatorContext context;
    JMSTestBase testBase;
    MessageProducer producer;
    Session session;
    Connection connection;

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
      operator = new JMSObjectInputOperator();
      operator.setSubject("TEST.FOO");
      operator.getConnectionFactoryProperties().put(JMSTestBase.AMQ_BROKER_URL, "vm://localhost");

      sink = new CollectorTestSink<Object>();
      operator.output.setSink(sink);
      operator.setup(context);
      operator.activate(context);
    }

    @Override
    protected void finished(Description description)
    {
      try {
        // Clean up
        session.close();
        connection.close();
      } catch (JMSException ex) {
        DTThrowable.rethrow(ex);
      }
      operator.deactivate();
      operator.teardown();
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
        testBase.afterTest();
      } catch (Exception e) {
        DTThrowable.rethrow(e);
      }
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testMapMsgInput() throws Exception
  {
    produceMsg();
    createMapMsgs(10);
    Thread.sleep(1000);
    testMeta.operator.emitTuples();
    Assert.assertEquals("num of messages", 10, testMeta.sink.collectedTuples.size());
  }

  @Test
  public void testStringMsgInput() throws Exception
  {
    produceMsg();
    createStringMsgs(10);
    Thread.sleep(1000);
    testMeta.operator.emitTuples();
    Assert.assertEquals("num of messages", 10, testMeta.sink.collectedTuples.size());
  }

  @Test
  public void testStreamMsgInput() throws Exception
  {
    produceMsg();
    createStreamMsgs(10);
    Thread.sleep(1000);
    testMeta.operator.emitTuples();
    Assert.assertEquals("num of messages", 10, testMeta.sink.collectedTuples.size());
  }

  @Test
  public void testByteInput() throws Exception
  {
    produceMsg();
    createByteMsgs(10);
    Thread.sleep(1000);
    testMeta.operator.emitTuples();
    Assert.assertEquals("num of messages", 10, testMeta.sink.collectedTuples.size());
  }

  @Test
  public void testObjectInput() throws Exception
  {
    produceMsg();
    createObjectMsgs(10);
    Thread.sleep(1000);
    testMeta.operator.emitTuples();
    Assert.assertEquals("num of messages", 10, testMeta.sink.collectedTuples.size());
  }

  private void produceMsg() throws Exception
  {
    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

    // Create a Connection
    testMeta.connection = connectionFactory.createConnection();
    testMeta.connection.start();

    // Create a Session
    testMeta.session = testMeta.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // Create the destination (Topic or Queue)
    Destination destination = testMeta.session.createQueue("TEST.FOO");

    // Create a MessageProducer from the Session to the Topic or Queue
    testMeta.producer = testMeta.session.createProducer(destination);
    testMeta.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
  }

  private void createMapMsgs(int numMessages) throws Exception
  {
    for (int i = 0; i < numMessages; i++) {
      MapMessage message = testMeta.session.createMapMessage();
      message.setString("userId", "abc" + i);
      message.setString("chatMessage", "hi" + i);
      testMeta.producer.send(message);
    }
  }

  private void createStringMsgs(int numMessages) throws Exception
  {
    String text = "Hello world! From tester producer";
    TextMessage message = testMeta.session.createTextMessage(text);
    for (int i = 0; i < numMessages; i++) {
      testMeta.producer.send(message);
    }
  }

  private void createStreamMsgs(int numMessages) throws Exception
  {
    Long value = 1013L;
    StreamMessage message = testMeta.session.createStreamMessage();
    message.writeObject(value);
    for (int i = 0; i < numMessages; i++) {
      testMeta.producer.send(message);
    }
  }

  private void createByteMsgs(int numMessages) throws Exception
  {
    BytesMessage message = testMeta.session.createBytesMessage();
    for (int i = 0; i < numMessages; i++) {
      message.writeBytes(("Message: " + i).getBytes());
      message.setIntProperty("counter", i);
      message.setJMSCorrelationID("MyCorrelationID");
      message.setJMSReplyTo(new ActiveMQQueue("MyReplyTo"));
      message.setJMSType("MyType");
      message.setJMSPriority(5);
      testMeta.producer.send(message);
    }
  }

  private void createObjectMsgs(int numMessages) throws Exception
  {
    ObjectMessage message = testMeta.session.createObjectMessage();
    message.setObject("Test for Object Messages");
    for (int i = 0; i < numMessages; i++) {
      testMeta.producer.send(message);
    }
  }

}
