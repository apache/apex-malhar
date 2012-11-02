/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InjectConfig;
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
 *  @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ActiveMQBase
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQBase.class);
  private transient Connection connection;
  private transient Session session;
  private transient Destination destination;

  // Config parameters that user can set.
  @NotNull
  @InjectConfig(key = "user")
  private String user = "";
  @NotNull
  @InjectConfig(key = "password")
  private String password = "";
  @NotNull
  @InjectConfig(key = "url")
  private String url;
  @InjectConfig(key = "ackMode")
  private String ackMode = "CLIENT_ACKNOWLEDGE";
  @InjectConfig(key = "clientId")
  private String clientId = "TestClient";
  @InjectConfig(key = "subject")
  private String subject = "TEST.FOO";
  @InjectConfig(key = "batch")
  private int batch = 10;
  @InjectConfig(key = "messageSize")
  private int messageSize = 255;
  @InjectConfig(key = "durable")
  private boolean durable = false;
  @InjectConfig(key = "topic")
  private boolean topic = false;
  @InjectConfig(key = "transacted")
  private boolean transacted = false;
  @InjectConfig(key = "verbose")
  private boolean verbose = false;

  public Connection getConnection()
  {
    return connection;
  }

  public Session getSession()
  {
    return session;
  }

  public Destination getDestination()
  {
    return destination;
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
   *  Get session acknowledge.
   *  Converts acknowledge string into JMS Session variable.
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
   *  Connection specific setup for ActiveMQ.
   *
   *  @throws JMSException
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
    connection.start();

    // Create session
    session = connection.createSession(transacted, getSessionAckMode(ackMode));

    // Create destination
    destination = topic
                  ? session.createTopic(subject)
                  : session.createQueue(subject);
  }

  /**
   *  cleanup connection resources.
   */
  public void cleanup() // where should be called TBD
  {
    logger.debug("cleanup got called from {}", this);

    try {
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
