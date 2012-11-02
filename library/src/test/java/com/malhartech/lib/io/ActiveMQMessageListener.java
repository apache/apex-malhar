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
 *  This is the AcctiveMQ message listener (i.e. consumer) outside of Malhar/Hadoop.
 *
 *  @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQMessageListener implements MessageListener, Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQMessageListener.class);
  private Connection connection;
  private Session session;
  private MessageConsumer consumer;
  private Destination destination;
  private int countMessages = 0;
  public HashMap<Integer, String> receivedData = new HashMap<Integer, String>();
  private String user = "";
  private String password = "";
  private String url = "tcp://localhost:61617";
  private int ackMode = Session.CLIENT_ACKNOWLEDGE;
  private String subject = "TEST.FOO";
  private int batch = 10;
  private int messageSize = 255;
  private long maximumReceiveMessages = 20; // 0 means unlimitted, this has to run in seperate thread for unlimitted
  private boolean durable = false;
  private boolean topic = false;
  private boolean transacted = false;
  private boolean verbose = false;
  private String consumerName = "Consumer1";

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

      logger.debug("Received a TextMessage: {}", msg);
    }
    else {
      throw new IllegalArgumentException("Unhandled message type " + message.getClass().getName());
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