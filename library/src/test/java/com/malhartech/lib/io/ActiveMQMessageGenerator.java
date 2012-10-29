/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.util.HashMap;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the message generator outside of Malhar/Hadoop.
 * This generates data and send to ActiveMQ message bus so that Malhar input adapter for ActiveMQ can receive it.
 *
 * @author Locknath <locknath@malhar-inc.com>
 */
public class ActiveMQMessageGenerator
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQMessageGenerator.class);
  private Connection connection;
  private Session session;
  private Destination destination;
  private MessageProducer producer;
  public HashMap<Integer, String> sendData = new HashMap<Integer, String>();
  public int sendCount = 0;
  private int debugMessageCount = 0;
  private ActiveMQBase amqConfig;

  public ActiveMQMessageGenerator(ActiveMQBase config)
  {
    amqConfig = config;
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
    connectionFactory = new ActiveMQConnectionFactory(
            amqConfig.getUser(),
            amqConfig.getPassword(),
            amqConfig.getUrl());

    connection = connectionFactory.createConnection();
    connection.start();

    // Create session
    session = connection.createSession(amqConfig.isTransacted(), amqConfig.getSessionAckMode(amqConfig.getAckMode()));

    // Create destination
    destination = amqConfig.isTopic()
                  ? session.createTopic(amqConfig.getSubject())
                  : session.createQueue(amqConfig.getSubject());

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
    long messageCount = amqConfig.getMaximumMessage();
    for (int i = 1; i <= messageCount || messageCount == 0; i++) {

      // Silly message
      String myMsg = "My TestMessage " + i;
      //String myMsg = "My TestMessage " + i + " sent at " + new Date();

      int messageSize = amqConfig.getMessageSize();
      if (myMsg.length() > messageSize) {
        myMsg = myMsg.substring(0, messageSize);
      }

      TextMessage message = session.createTextMessage(myMsg);

      producer.send(message);
      // store it for testing later
      sendData.put(i, myMsg);
      sendCount++;

      if (amqConfig.isVerbose()) {
        String msg = message.getText();
        if (msg.length() > messageSize) {
          msg = msg.substring(0, messageSize) + "...";
        }
        if (i <= debugMessageCount) {
          System.out.println("[" + this + "] Sending message from generator: '" + msg + "'");
        }
      }

      if (amqConfig.isTransacted()) {
        if (i <= debugMessageCount) {
          System.out.println("[" + this + "] Committing " + messageCount + " messages");
        }
        session.commit();
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
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }
}
