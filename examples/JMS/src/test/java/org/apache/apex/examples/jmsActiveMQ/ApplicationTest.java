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
package org.apache.apex.examples.jmsActiveMQ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);

  private static final String FILE_NAME = "test";
  private static final String FILE_DIR = "target/jmsActiveMQ";
  private static final String FILE_PATH = FILE_DIR + "/" + FILE_NAME + ".0";     // first part

  private String qNameToUse;
  private String brokerURL;

  private Connection connection;
  private MessageProducer producer;
  private Session session;
  private BrokerService broker;
  private Configuration conf;
  private static String[] lines = {"1st line", "2nd line", "3rd line", "4th line", "5th line"};

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      File file = new File(FILE_PATH);
      file.delete();
      getConfig();
      createAMQClient(brokerURL);
      //write messages to the ActiveMQ Queue
      writeToQueue();
      closeClient();
      //run app asynchronously; terminate after results are checked
      LocalMode.Controller lc = asyncRun();
      //check for presence of output file
      chkOutput();
      //compare output lines to input
      compare();
      lc.shutdown();
      broker.stop();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  /**
   * Start the embedded Active MQ broker for our test.
   *
   * @throws Exception
   */
  private void startEmbeddedActiveMQBroker() throws Exception
  {
    broker = new BrokerService();
    String brokerName = "ActiveMQOutputOperator-broker";
    broker.setBrokerName(brokerName);
    broker.getPersistenceAdapter().setDirectory(new File("target/activemq-data/" +
        broker.getBrokerName() + '/' +
        org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter.class.getSimpleName()).getAbsoluteFile());
    broker.addConnector("tcp://localhost:61617?broker.persistent=false");
    broker.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 1024);  // 1GB
    broker.getSystemUsage().getTempUsage().setLimit(100 * 1024 * 1024);    // 100MB
    broker.setDeleteAllMessagesOnStartup(true);
    broker.start();
  }

  /**
   * Create an embedded AMQ broker and a client as the producer for our test.
   * Create a queue with the supplied queue name.
   *
   * @throws Exception
   */
  private void createAMQClient(String brokerURL) throws Exception
  {
    startEmbeddedActiveMQBroker();

    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);

    // Create a Connection
    connection = connectionFactory.createConnection();
    connection.start();

    // Create a Session
    session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

    // Create the destination queue
    Destination destination = session.createQueue(qNameToUse);

    // Create a MessageProducer from the Session to the Topic or Queue
    producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
  }

  private void writeMsg(String[] msgs) throws JMSException
  {
    for (String text : msgs) {
      TextMessage message = session.createTextMessage(text);
      producer.send(message);
    }
  }

  /**
   * Write the test lines to the queue
   *
   * @throws JMSException
   */
  private void writeToQueue() throws JMSException
  {
    writeMsg(lines);
    LOG.debug("Sent messages to topic {}", qNameToUse);
  }

  private void closeClient() throws JMSException
  {
    session.close();
    connection.close();
  }

  private void getConfig()
  {
    conf = new Configuration(false);

    // read config values from the properties.xml file
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));

    qNameToUse = conf.get("dt.operator.amqIn.prop.subject");
    brokerURL = conf.get("dt.operator.amqIn.prop.connectionFactoryProperties.brokerURL");

    // one can also set or override values in code as below
    String pre = "dt.operator.fileOut.prop.";
    conf.set(pre + "filePath", FILE_DIR);
  }

  private static void chkOutput() throws Exception
  {
    File file = new File(FILE_PATH);
    final int MAX = 60;
    for (int i = 0; i < MAX && (!file.exists()); ++i) {
      LOG.debug("Sleeping, i = {}", i);
      Thread.sleep(1000);
    }
    if (!file.exists()) {
      String msg = String.format("Error: %s not found after %d seconds%n", FILE_PATH, MAX);
      throw new RuntimeException(msg);
    }
  }

  private void compare() throws Exception
  {
    //read output file
    File file = new File(FILE_PATH);
    BufferedReader br = new BufferedReader(new FileReader(file));

    HashSet<String> set = new HashSet<String>();
    String line;
    while (null != (line = br.readLine())) {
      set.add(line);
    }
    br.close();

    //now delete the file, we don't need it anymore
    Assert.assertTrue("Deleting " + file, file.delete());

    //compare
    Assert.assertEquals("number of lines", lines.length, set.size());
    for (int i = 0; i < lines.length; ++i) {
      Assert.assertTrue("set contains " + lines[i], set.remove(lines[i]));
    }
  }

  private LocalMode.Controller asyncRun() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new ActiveMQApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    return lc;
  }

}
