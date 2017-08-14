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
package org.apache.apex.malhar.lib.testbench;

import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * This is the message generator outside of Malhar/Hadoop. This generates data
 * and send to ActiveMQ message bus so that Malhar input adapter for ActiveMQ
 * can receive it.
 *
 */
public class ActiveMQMessageGenerator
{
  private static final Logger logger = LoggerFactory
      .getLogger(ActiveMQMessageGenerator.class);
  private Connection connection;
  private Session session;
  private Destination destination;
  private MessageProducer producer;
  public HashMap<Integer, String> sendData = new HashMap<Integer, String>();
  public int sendCount = 0;
  private int debugMessageCount = 0;
  private String user = "";
  private String password = "";
  private String url = "tcp://localhost:61617";
  private int ackMode = Session.CLIENT_ACKNOWLEDGE;
  private String subject = "TEST.FOO";
  private int messageSize = 255;
  private long maximumSendMessages = 20; // 0 means unlimitted, this has to run
                                         // in seperate thread for unlimitted
  private boolean topic = false;
  private boolean transacted = false;
  private boolean verbose = false;

  public ActiveMQMessageGenerator()
  {
  }

  public void setDebugMessageCount(int count)
  {
    debugMessageCount = count;
  }

  /**
   * Setup connection, producer, consumer so on.
   *
   * @throws JMSException
   */
  public void setupConnection() throws JMSException
  {
    // Create connection
    ActiveMQConnectionFactory connectionFactory;
    connectionFactory = new ActiveMQConnectionFactory(user, password, url);

    connection = connectionFactory.createConnection();
    connection.start();

    // Create session
    session = connection.createSession(transacted, ackMode);

    // Create destination
    destination = topic ? session.createTopic(subject) : session
        .createQueue(subject);

    // Create producer
    producer = session.createProducer(destination);
  }

  /**
   * Generate message and send it to ActiveMQ message bus.
   *
   * @throws Exception
   */
  public void sendMessage() throws Exception
  {
    for (int i = 1; i <= maximumSendMessages || maximumSendMessages == 0; i++) {

      // Silly message
      String myMsg = "My TestMessage " + i;
      // String myMsg = "My TestMessage " + i + " sent at " + new Date();

      if (myMsg.length() > messageSize) {
        myMsg = myMsg.substring(0, messageSize);
      }

      TextMessage message = session.createTextMessage(myMsg);

      producer.send(message);
      // store it for testing later
      sendData.put(i, myMsg);
      sendCount++;

      if (verbose) {
        String msg = message.getText();
        if (msg.length() > messageSize) {
          msg = msg.substring(0, messageSize) + "...";
        }
        if (i <= debugMessageCount) {
          System.out.println("[" + this + "] Sending message from generator: '"
              + msg + "'");
        }
      }
    }
  }

  /**
   * Close connection resources.
   */
  public void closeConnection()
  {
    try {
      producer.close();
      session.close();
      connection.close();
    } catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }
}
