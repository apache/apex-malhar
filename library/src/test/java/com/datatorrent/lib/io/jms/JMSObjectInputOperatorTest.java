/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io.jms;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ObjectUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSObjectInputOperatorTest
{

  public static class TestMeta extends TestWatcher
  {
    String baseDir;
    String recoveryDir;
    JMSInputObjectOperator operator;
    CollectorTestSink<Object> sink;
    Context.OperatorContext context;
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
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      String methodName = description.getMethodName();
      String className = description.getClassName();
      baseDir = "target/" + className + "/" + methodName;
      recoveryDir = baseDir + "/" + "recovery";

      Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
      attributeMap.put(Context.OperatorContext.SPIN_MILLIS, 500);

      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributeMap);
      operator = new JMSInputObjectOperator();
      operator.getConnectionFactoryProperties().put(JMSTestBase.AMQ_BROKER_URL, "vm://localhost");
      ((IdempotentStorageManager.FSIdempotentStorageManager)operator.getIdempotentStorageManager()).setRecoveryPath(recoveryDir);

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
      }
      catch (JMSException ex) {
        DTThrowable.rethrow(ex);
      }
      operator.deactivate();
      operator.teardown();
      try {
        FileUtils.deleteDirectory(new File(baseDir));
        testBase.afterTest();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
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
    Long value = (long)1013;
    StreamMessage message=testMeta.session.createStreamMessage();
     message.writeObject(value);
    for (int i = 0; i < numMessages; i++) {
      testMeta.producer.send(message);
    }

  }

}
