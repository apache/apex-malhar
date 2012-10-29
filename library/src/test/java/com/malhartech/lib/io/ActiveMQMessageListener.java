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
 * This is the AcctiveMQ message listener (i.e. consumer) outside of Malhar/Hadoop.
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQMessageListener implements MessageListener, Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQMessageListener.class);
  private Connection connection;
  private Session session;
  private MessageConsumer consumer;
  private Destination destination;
  private int countMessages = 0;
  private long maximumReceiveMessages = 0;
  private ActiveMQBase amqConfig;
  public HashMap<Integer, String> receivedData = new HashMap<Integer, String>();

  public ActiveMQMessageListener(ActiveMQBase config)
  {
    amqConfig = config;
    maximumReceiveMessages = amqConfig.getMaximumReceiveMessages();
  }

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

    consumer = (amqConfig.isDurable() && amqConfig.isTopic())
               ? session.createDurableSubscriber((Topic)destination, amqConfig.getConsumerName())
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
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
      return;
    }

    //System.out.println("we are in onMessage");
    if (message instanceof TextMessage) {
      TextMessage txtMsg = (TextMessage)message;
      String msg = null;
      try {
        msg = txtMsg.getText();
        receivedData.put(new Integer(countMessages), msg);
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }

      System.out.println("Received a TextMessage: '" + msg + "'");
    }
    else {
      System.out.println(String.format("Not a string instance (%s)", message.toString()));
    }

  }

  @Override
  public void run()
  {
    try {
      Thread.sleep(2000);  // how long this should be?
    }
    catch (InterruptedException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  public void closeConnection()
  {
    try {
      consumer.close();
      session.close();
      connection.close();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }
}