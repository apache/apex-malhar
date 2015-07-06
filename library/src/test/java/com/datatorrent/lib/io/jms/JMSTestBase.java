/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.jms;

import java.io.File;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for JMS operators test. <br/>
 * Setup the JMS service to serve the test
 */
public class JMSTestBase
{
  public static String AMQ_BROKER_URL = "brokerURL";
  private BrokerService broker;


  /**
   * Start JMS broker from the Testcase.
   *
   * @throws Exception
   */
  private void startJMSService() throws Exception
  {
    broker = new BrokerService();
    String brokerName = "ActiveMQOutputOperator-broker";
    broker.setBrokerName(brokerName);
    broker.getPersistenceAdapter().setDirectory(new File("target/activemq-data/" + broker.getBrokerName() + '/' + org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter.class.getSimpleName()).getAbsoluteFile());
    broker.addConnector("tcp://localhost:61617?broker.persistent=false");
    broker.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 1024);  // 1GB
    broker.getSystemUsage().getTempUsage().setLimit(100 * 1024 * 1024);    // 100MB
    broker.setDeleteAllMessagesOnStartup(true);
    broker.start();
  }

  public void produceMsg(String text) throws Exception
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
    TextMessage message = session.createTextMessage(text);
    producer.send(message);

    // Clean up
    session.close();
    connection.close();
  }

  @Before
  public void beforTest() throws Exception
  {
    startJMSService();
  }

  @After
  public void afterTest() throws Exception
  {
    broker.stop();
    FileUtils.deleteDirectory(new File("target/activemq-data").getAbsoluteFile());
  }
}