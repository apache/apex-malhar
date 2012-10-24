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

/**
 * This is the message generator outside of Malhar/Hadoop.
 * This generates data and send to ActiveMQ message bus so that Malhar input adapter for ActiveMQ can receive it.
 *
 * @author Locknath <locknath@malhar-inc.com>
 */
public class ActiveMQMessageGenerator
{
  private OperatorConfiguration config;
  private Connection connection;
  private Session session;
  private Destination destination;
  private MessageProducer producer;
  public HashMap<Integer, String> sendData = new HashMap<Integer, String>();
  public int sendCount = 0;
  private int debugMessageCount = 0;

  public void setDebugMessageCount(int count)
  {
    debugMessageCount = count;
  }

  public ActiveMQMessageGenerator(OperatorConfiguration config)
  {
    this.config = config;
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
    //Destination destination;
    if (config.getBoolean("topic", false)) {
      destination = session.createTopic(config.get("subject"));
    }
    else {
      destination = session.createQueue(config.get("subject"));
    }

    // Create producer
    producer = session.createProducer(destination);
  }

  public void sendMessage() throws Exception
  {
    int messageCount = config.getInt("maximumMessages", 0);
    for (int i = 0; i < messageCount || messageCount == 0; i++) {

      // Silly message
      String myMsg = "My TestMessage " + i;
      //String myMsg = "My TestMessage " + i + " sent at " + new Date();

      int messageSize = config.getInt("messageSize", 0);
      if (myMsg.length() > messageSize) {
        myMsg = myMsg.substring(0, messageSize);
      }

      TextMessage message = session.createTextMessage(myMsg);

      producer.send(message);
      // store it for testing later
      sendData.put(i, myMsg);
      sendCount++;

      if (config.getBoolean("verbose", false)) {
        String msg = message.getText();
        if (msg.length() > messageSize) {
          msg = msg.substring(0, messageSize) + "...";
        }
        if (i < debugMessageCount) {
          System.out.println("[" + this + "] Sending message from generator: '" + msg + "'");
        }
      }

      if (config.getBoolean("transacted", false)) {
        if (i < debugMessageCount) {
          System.out.println("[" + this + "] Committing " + messageCount + " messages");
        }
        session.commit();
      }
    }
  }

  public void closeConnection()
  {
    try {
      producer.close();
      session.close();
      connection.close();
    }
    catch (JMSException ex) {
      Logger.getLogger(ActiveMQMessageGenerator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
