/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InjectConfig;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQBase
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQBase.class);
  private boolean isProducer;
  private Connection connection;
  private Session session;
  private MessageProducer producer;
  private MessageProducer replyProducer;
  private MessageConsumer consumer;
  private Destination destination;
  private long messagesReceived = 0;
  @InjectConfig(key = "usr")
  private String user = "";
  @InjectConfig(key = "password")
  private String password = "";
  @InjectConfig(key = "url")
  private String url;
  @InjectConfig(key = "ackMode")
  private String ackMode;
  @InjectConfig(key = "clientId")
  private String clientId;
  @InjectConfig(key = "consumerName")
  private String consumerName;
  @InjectConfig(key = "subject")
  private String subject = "TEST.FOO";
  @InjectConfig(key = "batch")
  private int batch = 10;
  @InjectConfig(key = "messageSize")
  private int messageSize = 255;
  @InjectConfig(key = "maximumMessage")
  private long maximumMessage = 0; // means unlimitted
  @InjectConfig(key = "maximumSendMessages")
  private long maximumSendMessages = 0; // means unlimitted
  @InjectConfig(key = "maximumReceiveMessages")
  private long maximumReceiveMessages = 0; // means unlimitted
  @InjectConfig(key = "durable")
  private boolean durable = false;
  @InjectConfig(key = "topic")
  private boolean topic = false;
  @InjectConfig(key = "transacted")
  private boolean transacted = false;
  @InjectConfig(key = "verbose")
  private boolean verbose = false;


  ActiveMQBase(boolean isProducer)
  {
    this.isProducer = isProducer;
  }

  public boolean isIsProducer()
  {
    return isProducer;
  }

  public void setIsProducer(boolean isProducer)
  {
    this.isProducer = isProducer;
  }

  public Connection getConnection()
  {
    return connection;
  }

  public void setConnection(Connection connection)
  {
    this.connection = connection;
  }

  public Session getSession()
  {
    return session;
  }

  public void setSession(Session session)
  {
    this.session = session;
  }

  public MessageProducer getProducer()
  {
    return producer;
  }

  public void setProducer(MessageProducer producer)
  {
    this.producer = producer;
  }

  public MessageProducer getReplyProducer()
  {
    return replyProducer;
  }

  public void setReplyProducer(MessageProducer replyProducer)
  {
    this.replyProducer = replyProducer;
  }

  public MessageConsumer getConsumer()
  {
    return consumer;
  }

  public void setConsumer(MessageConsumer consumer)
  {
    this.consumer = consumer;
  }

  public Destination getDestination()
  {
    return destination;
  }

  public void setDestination(Destination destination)
  {
    this.destination = destination;
  }

  public long getMessagesReceived()
  {
    return messagesReceived;
  }

  public void setMessagesReceived(long messagesReceived)
  {
    this.messagesReceived = messagesReceived;
  }

  public String getUser()
  {
    return user;
  }

  public void setUser(String user)
  {
    this.user = user;
  }

  public String getPassword()
  {
    return password;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }

  public String getUrl()
  {
    return url;
  }

  public void setUrl(String url)
  {
    this.url = url;
  }

  public String getAckMode()
  {
    return ackMode;
  }

  public void setAckMode(String ackMode)
  {
    this.ackMode = ackMode;
  }

  public String getClientId()
  {
    return clientId;
  }

  public void setClientId(String clientId)
  {
    this.clientId = clientId;
  }

  public String getConsumerName()
  {
    return consumerName;
  }

  public void setConsumerName(String consumerName)
  {
    this.consumerName = consumerName;
  }

  public String getSubject()
  {
    return subject;
  }

  public void setSubject(String subject)
  {
    this.subject = subject;
  }

  public int getBatch()
  {
    return batch;
  }

  public void setBatch(int batch)
  {
    this.batch = batch;
  }

  public int getMessageSize()
  {
    return messageSize;
  }

  public void setMessageSize(int messageSize)
  {
    this.messageSize = messageSize;
  }

  public long getMaximumMessage()
  {
    return maximumMessage;
  }

  public void setMaximumMessage(long maximumMessage)
  {
    this.maximumMessage = maximumMessage;
  }

  public long getMaximumSendMessages()
  {
    return maximumSendMessages;
  }

  public void setMaximumSendMessages(long maximumSendMessages)
  {
    this.maximumSendMessages = maximumSendMessages;
  }

  public long getMaximumReceiveMessages()
  {
    return maximumReceiveMessages;
  }

  public void setMaximumReceiveMessages(long maximumReceiveMessages)
  {
    this.maximumReceiveMessages = maximumReceiveMessages;
  }

  public boolean isDurable()
  {
    return durable;
  }

  public void setDurable(boolean durable)
  {
    this.durable = durable;
  }

  public boolean isTopic()
  {
    return topic;
  }

  public void setTopic(boolean topic)
  {
    this.topic = topic;
  }

  public boolean isTransacted()
  {
    return transacted;
  }

  public void setTransacted(boolean transacted)
  {
    this.transacted = transacted;
  }

  public boolean isVerbose()
  {
    return verbose;
  }

  public void setVerbose(boolean verbose)
  {
    this.verbose = verbose;
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
   * Connection specific setup for ActiveMQ.
   *
   * @param config
   * @throws JMSException
   */
  public void setupConnection() throws JMSException
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
    destination = topic
                              ? session.createTopic(subject)
                              : session.createQueue(subject);


    if (isProducer) {
      // Create producer
      producer = session.createProducer(destination);
      //producer.setDeliveryMode(DeliveryMode.PERSISTENT);
    }
    else {
      replyProducer = session.createProducer(null);

      consumer = (durable && topic)
                 ? session.createDurableSubscriber((Topic)destination, consumerName)
                 : session.createConsumer(destination);
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
          if (verbose) {
            System.out.println("Commiting transaction for last " + batch + " messages; messages so far = " + messagesReceived);
          }
          session.commit();
        }
      }
      else if (getSessionAckMode(ackMode) == Session.CLIENT_ACKNOWLEDGE) {
        // we can use window boundary to ack the message.
        if ((messagesReceived % batch) == 0) {
          if (verbose) {
            System.out.println("Acknowledging last " + batch + " messages; messages so far = " + messagesReceived);
          }
          message.acknowledge();
        }
      }
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   * cleanup implementation.
   */
  public void cleanup()
  {
    logger.debug("cleanup got called from {}", this);

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
      logger.debug(ex.getLocalizedMessage());
    }
  }
}
