/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.validation.constraints.NotNull;
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
  //private boolean isProducer;
  protected Connection connection;
  protected Session session;
 // private MessageProducer producer;
 // private MessageProducer replyProducer;
 // private MessageConsumer consumer;

  protected Destination destination;

  @NotNull
  private String user;

  @NotNull
  private String password = "";

  @NotNull
  private String url;

  protected String ackMode;
  private String clientId;

  private String subject = "TEST.FOO";
  protected int batch = 10;
  private int messageSize = 255;
  private long maximumMessage = 0; // means unlimitted

  private long maximumReceiveMessages = 0; // means unlimitted
  protected boolean durable = false;
  protected boolean topic = false;
  protected boolean transacted = false;
  protected boolean verbose = false;


  ActiveMQBase()
  {
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


  public Destination getDestination()
  {
    return destination;
  }

  public void setDestination(Destination destination)
  {
    this.destination = destination;
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
   * @throws JMSException
   */
  public void createConnection() throws JMSException
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

/*
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
    */
  }

  /**
   * cleanup implementation.
   */
  public void cleanup()
  {
    logger.debug("cleanup got called from {}", this);

    try {
    /*  if (isProducer) {
        producer.close();
        producer = null;
      }
      else {
        replyProducer.close();
        replyProducer = null;
        consumer.close();
        consumer = null;
      }
*/

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
