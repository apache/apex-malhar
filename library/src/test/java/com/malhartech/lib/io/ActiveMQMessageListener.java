/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.OperatorConfiguration;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.LoggerFactory;

/**
 * This is the AcctiveMQ message listener (i.e. consumer) outside of Malhar/Hadoop.
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQMessageListener implements MessageListener, Runnable
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ActiveMQMessageListener.class);
  private OperatorConfiguration config;
  private Connection connection;
  private Session session;
  private MessageConsumer consumer;
  HashMap<Integer, String> receivedData = new HashMap<Integer, String>();
  private int countMessages = 0;
  private int maximumReceiveMessages = 0;

  public ActiveMQMessageListener(OperatorConfiguration config)
  {
    this.config = config;
    maximumReceiveMessages = config.getInt("maximumReceiveMessages", 0);
  }

  public void setupConnection() throws JMSException
  {
    // Create connection
    ActiveMQConnectionFactory connectionFactory;
    connectionFactory = new ActiveMQConnectionFactory(
            config.get("user"),
            config.get("password"),
            config.get("url"));

    connection = connectionFactory.createConnection();
    connection.start();

    // Create session
    session = connection.createSession(config.getBoolean("transacted", false), Session.AUTO_ACKNOWLEDGE);

    // Create destination
    Destination destination;
    if (config.getBoolean("topic", false)) {
      destination = session.createTopic(config.get("subject"));
    }
    else {
      destination = session.createQueue(config.get("subject"));
    }

    if (config.getBoolean("durable", false) && config.getBoolean("topic", false)) {
      consumer = session.createDurableSubscriber((Topic)destination, config.get("consumerName"));
    }
    else {
      consumer = session.createConsumer(destination);
    }
    consumer.setMessageListener(this);
  }

  @Override
  public void onMessage(Message message)
  {
    // Stop listener if captured maximum messages.
    if (countMessages++ >= maximumReceiveMessages && maximumReceiveMessages != 0){
      try {
        logger.warn("Reached maximum receive messages of {}", maximumReceiveMessages);
        consumer.setMessageListener(null);
      }
      catch (JMSException ex) {
        Logger.getLogger(ActiveMQMessageListener.class.getName()).log(Level.SEVERE, null, ex);
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
        Logger.getLogger(ActiveMQOutputOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
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
      Logger.getLogger(ActiveMQOutputOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
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
      Logger.getLogger(ActiveMQMessageGenerator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

}