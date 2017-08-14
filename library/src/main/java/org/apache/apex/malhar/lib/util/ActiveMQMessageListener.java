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
package org.apache.apex.malhar.lib.util;

import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 *  This is the AcctiveMQ message listener (consumer) outside of Malhar/Hadoop.
 *
 * @since 0.3.3
 */
public class ActiveMQMessageListener implements MessageListener, Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQMessageListener.class);
  private Connection connection;
  private Session session;
  private MessageConsumer consumer;
  private Destination destination;
  protected int countMessages = 0;
  public HashMap<Integer, Object> receivedData = new HashMap<Integer, Object>();
  private String user = "";
  private String password = "";
  private String url = "tcp://localhost:61617";
  private int ackMode = Session.CLIENT_ACKNOWLEDGE;
  private String subject = "TEST.FOO";
  @SuppressWarnings("unused")
  private int batch = 10;
  @SuppressWarnings("unused")
  private int messageSize = 255;
  private long maximumReceiveMessages = 20; // 0 means unlimitted, this has to run in seperate thread for unlimitted
  private boolean durable = false;
  private boolean topic = false;
  private boolean transacted = false;
  @SuppressWarnings("unused")
  private boolean verbose = false;
  private String consumerName = "Consumer1";

  public void setUser(String user)
  {
    this.user = user;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }

  public void setUrl(String url)
  {
    this.url = url;
  }

  public void setAckMode(int ackMode)
  {
    this.ackMode = ackMode;
  }

  public void setSubject(String subject)
  {
    this.subject = subject;
  }

  public void setBatch(int batch)
  {
    this.batch = batch;
  }

  public void setMessageSize(int messageSize)
  {
    this.messageSize = messageSize;
  }

  public void setMaximumReceiveMessages(long maximumReceiveMessages)
  {
    this.maximumReceiveMessages = maximumReceiveMessages;
  }

  public void setDurable(boolean durable)
  {
    this.durable = durable;
  }

  public void setTopic(boolean topic)
  {
    this.topic = topic;
  }

  public void setTransacted(boolean transacted)
  {
    this.transacted = transacted;
  }

  public void setVerbose(boolean verbose)
  {
    this.verbose = verbose;
  }

  public void setConsumerName(String consumerName)
  {
    this.consumerName = consumerName;
  }


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
    destination = topic
                  ? session.createTopic(subject)
                  : session.createQueue(subject);

    // Create consumer
    consumer = (durable && topic)
               ? session.createDurableSubscriber((Topic)destination, consumerName)
               : session.createConsumer(destination);

    consumer.setMessageListener(this);
  }

  @Override
  public void onMessage(Message message)
  {
    // Stop listener if captured maximum messages.
    if (countMessages++ >= maximumReceiveMessages && maximumReceiveMessages != 0) {
      try {
        logger.warn("Reached maximum receive messages of {}", maximumReceiveMessages);
        consumer.setMessageListener(null);
      } catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
      return;
    }
  }

  @Override
  public void run()
  {
    try {
      Thread.sleep(2000);  // how long this should be?
    } catch (InterruptedException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  public void closeConnection()
  {
    try {
      consumer.close();
      session.close();
      connection.close();
    } catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }
}
