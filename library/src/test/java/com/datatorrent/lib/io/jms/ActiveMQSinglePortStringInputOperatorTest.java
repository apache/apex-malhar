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
package com.datatorrent.lib.io.jms;

import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;


/**
 * Unit test to verify ActiveMQ input operator adapter reads a string message from the ActiveMQ.
 *
 */
public class ActiveMQSinglePortStringInputOperatorTest extends ActiveMQOperatorTestBase
{
  private static final int MSG_LENGTH = 10;

  @Test
  public void testStringMsgInput() throws Exception
  {

    ActiveMQSinglePortStringInputOperator oper = new ActiveMQSinglePortStringInputOperator();
    oper.getConnectionFactoryProperties().put(AMQ_BROKER_URL, "vm://localhost");
    oper.setMaximumReceiveMessages(100);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    oper.outputPort.setSink(sink);
    oper.setup(null);
    oper.activate(null);
    produceMsg();
    Thread.sleep(1000);
    oper.emitTuples();
    Thread.sleep(1000);
    Assert.assertEquals(MSG_LENGTH + " expected, but " + sink.collectedTuples.size() + " received", MSG_LENGTH, sink.collectedTuples.size());
    oper.cleanup();
    oper.teardown();
  }

  private void produceMsg() throws Exception
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
    for (int i = 0; i < MSG_LENGTH; i++) {
      producer.send(message);
    }

    // Clean up
    session.close();
    connection.close();

  }

}
