/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.OperatorConfiguration;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQHelper
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQHelper.class);
  private boolean isProducer;
  private Connection connection;
  private Session session;
  private MessageProducer producer;
  private MessageProducer replyProducer;
  private MessageConsumer consumer;
  private long messagesReceived = 0;
  private String user;
  private String password;
  private String url;
  private String ackMode;
  private String clientId;
  private String consumerName;
  private String subject;
  private long maximumMessage;
  private int receiveTimeOut;
  private int sleepTime;
  private int parallelThreads;
  private int batch;
  private int messageSize;
  private boolean durable;
  private boolean pauseBeforeShutdown;
  private boolean topic;
  private boolean transacted;
  private boolean verbose;

  //TBD deliveryMode
  public ActiveMQHelper(boolean isProd)
  {
    isProducer = isProd;
  }

  public Connection getConnection()
  {
    return connection;
  }

  /**
   * @return the session
   */
  public Session getSession()
  {
    return session;
  }

  /**
   * @return the producer
   */
  public MessageProducer getProducer()
  {
    return producer;
  }

  /**
   * @return the consumer
   */
  public MessageConsumer getConsumer()
  {
    return consumer;
  }

   long getMaximumMessage()
  {
    return maximumMessage;
  }

  /**
   * Get session acknowledge
   */
  public int getSessionAckMode(String ackMode)
  {
    if ("CLIENT_ACKNOWLEDGE".equals(ackMode)) {
      return Session.CLIENT_ACKNOWLEDGE;
    }
    else if ("AUTO_ACKNOWLEDGE".equals(ackMode)) {
      return Session.AUTO_ACKNOWLEDGE;
    }
    else if ("DUPS_OK_ACKNOWLEDGE".equals(ackMode)) {
      return Session.DUPS_OK_ACKNOWLEDGE;
    }
    else if ("SESSION_TRANSACTED".equals(ackMode)) {
      return Session.SESSION_TRANSACTED;
    }
    else {
      return Session.CLIENT_ACKNOWLEDGE; // default
    }
  }

  /**
   * Read configuration info and set default values if not set properly.
   *
   * @param config
   */
  public void readConfig(OperatorConfiguration config)
  {
    user = config.get("user");
    if (user == null) {
      user = "";
    }
    password = config.get("password");
    if (password == null) {
      password = "";
    }
    url = config.get("url");
    if (url == null) {
      url = "tcp://localhost:61616";
    }
    ackMode = config.get("ackMode");
    if (ackMode == null) {
      ackMode = "AUTO_ACKNOWLEDGE";
    }
    clientId = config.get("clientId");
    if (clientId == null) {
      clientId = "Producer";
    }
    consumerName = config.get("consumerName");
    if (consumerName == null) {
      consumerName = "ChetanConsumer";
    }
    subject = config.get("subject");
    if (subject == null) {
      subject = "TEST.FOO";
    }

    maximumMessage = config.getLong("maximumMessages", 10);
    receiveTimeOut = config.getInt("receiveTimeOut", 0);
    sleepTime = config.getInt("sleepTime", 1000);
    parallelThreads = config.getInt("parallelThreads", 1);
    batch = config.getInt("batch", 10);
    messageSize = config.getInt("messageSize", 225);

    durable = config.getBoolean("durable", false);
    pauseBeforeShutdown = config.getBoolean("pauseBeforeShutdown", true);
    topic = config.getBoolean("topic", false);
    transacted = config.getBoolean("transacted", false);
    verbose = config.getBoolean("verbose", false);

  }

  /**
   * Setup implementation.
   *
   * @param config
   * @throws FailedOperationException
   */
  public void setup(OperatorConfiguration config)
  {
    readConfig(config);
    try {
      setupConnection(config);
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   * Connection specific setup for ActiveMQ.
   *
   * @param config
   * @throws JMSException
   */
  private void setupConnection(OperatorConfiguration config) throws JMSException
  {
    // Create connection
    ActiveMQConnectionFactory connectionFactory;
    connectionFactory = new ActiveMQConnectionFactory(user, password, url);

    connection = connectionFactory.createConnection();
    if (durable && clientId != null) {
      connection.setClientID(clientId);
    }
    //connection.setExceptionListener(this); // Needed only for consumer which extends from ExceptionListener
    connection.start();

    // Create session
    session = connection.createSession(transacted, getSessionAckMode(ackMode));

    // Create destination
    Destination destination = topic ? session.createTopic(subject) : session.createQueue(subject);

    if (isProducer) {
      // Create producer
      producer = session.createProducer(destination);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);
    }
    else {
      replyProducer = session.createProducer(null);

      if (config.getBoolean("durable", false) && config.getBoolean("topic", false)) {
        consumer = session.createDurableSubscriber((Topic)destination, config.get("consumerName"));
      }
      else {
        consumer = session.createConsumer(destination);
      }
    }
  }

  public void handleMessage(Message message)
  {
    ++messagesReceived;
    try {
      if (message.getJMSReplyTo() != null) {
        // Send reply only if the replyTo destination is set
        replyProducer.send(message.getJMSReplyTo(), session.createTextMessage("Reply: " + message.getJMSMessageID()));
      }

      if (transacted) {
        if ((messagesReceived % batch) == 0) {
          System.out.println("Commiting transaction for last " + batch + " messages; messages so far = " + messagesReceived);
          session.commit();
        }
      }
      else if (getSessionAckMode(ackMode) == Session.CLIENT_ACKNOWLEDGE) {
        // we can use window boundary to ack the message.
        if ((messagesReceived % batch) == 0) {
          System.out.println("Acknowledging last " + batch + " messages; messages so far = " + messagesReceived);
          message.acknowledge();
        }
      }
    }
    catch (JMSException ex) {
      logger.error(null, ex);
    }
  }

  /**
   * teardown implementation.
   */
  public void teardown()
  {
    try {
      if (isProducer) {
        producer.close();
        producer = null;
      }
      else {
        replyProducer.close();
        replyProducer = null;
        consumer.close();
        consumer = null;
      }

      session.close();
      connection.close();
      session = null;
      connection = null;
    }
    catch (JMSException ex) {
      logger.error(null, ex);
    }
  }


}
