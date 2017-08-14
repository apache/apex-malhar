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
package org.apache.apex.malhar.lib.io.jms;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.beanutils.BeanUtils;

import com.google.common.collect.Maps;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * Base class for any JMS input or output adapter operator.
 * <p/>
 * Operators should not be derived from this,
 * rather from AbstractJMSInputOperator or AbstractJMSSinglePortInputOperator or AbstractJMSOutputOperator
 * or AbstractJMSSinglePortOutputOperator. This creates connection with a JMS broker.<br>
 *
 * <br>
 * Ports:<br>
 * None<br>
 * <br>
 * Properties:<br>
 * <b>connectionFactoryClass</b>: Connection factory of the JMS provider (Default is ActiveMQ)<br>
 * <b>connectionFactoryProperties</b>: Properties to initialize the connection factory (consult your providers documentation)<br>
 * <b>ackMode</b>: message acknowledgment mode<br>
 * <b>clientId</b>: client id<br>
 * <b>subject</b>: name of destination<br>
 * <b>durable</b>: flag to indicate durable consumer<br>
 * <b>topic</b>: flag to indicate if the destination is a topic or queue<br>
 * <b>transacted</b>: flag whether the messages should be transacted or not<br>
 * <br>
 * Compile time checks:<br>
 * usr should not be null<br>
 * password should not be null<br>
 * url should not be null<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * NA<br>
 * <br>
 *
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class JMSBase
{
  private static final Logger logger = LoggerFactory.getLogger(JMSBase.class);
  private transient Connection connection;
  private transient Session session;
  private transient Destination destination;

  @NotNull
  private ConnectionFactoryBuilder connectionFactoryBuilder;
  private String ackMode = "CLIENT_ACKNOWLEDGE";
  private String clientId;
  private String subject;
  private int batch = 10;
  private int messageSize = 255;
  private boolean durable = false;
  private boolean topic = false;
  private boolean verbose = false;
  protected boolean transacted = true;

  /**
   * Builder class that allows caller to build the connection factory (optional)
   *
   */
  public static interface ConnectionFactoryBuilder
  {

    /**
     * This method is called by the operator to return properly built
     * (authenticated, connected etc) connection factory
     *
     * @return properly built connection factory
     */
    public ConnectionFactory buildConnectionFactory();
  }

  /**
   * Default implementation for {@link ConnectionFactoryBuilder} that works for ActiveMQ
   *
   *
   */
  public static class DefaultConnectionFactoryBuilder implements ConnectionFactoryBuilder
  {
    protected String connectionFactoryClass;

    @NotNull
    protected Map<String, String> connectionFactoryProperties = Maps.newHashMap();

    /**
     * Get properties used to configure this DefaultConnectionFactoryBuilder instance
     *
     * @return Map of properties
     */
    public Map<String, String> getConnectionFactoryProperties()
    {
      return connectionFactoryProperties;
    }

    /**
     * Set properties used to configure this DefaultConnectionFactoryBuilder instance.
     * Note: previous properties are overwritten.
     *
     * @param connectionFactoryProperties
     */
    public void setConnectionFactoryProperties(Map<String, String> connectionFactoryProperties)
    {
      this.connectionFactoryProperties = connectionFactoryProperties;
    }

    /**
     * Get the fully qualified class-name of the connection factory that is used by this
     * builder to instantiate the connection factory
     *
     * @return fully qualified class-name
     */
    public String getConnectionFactoryClass()
    {
      return connectionFactoryClass;
    }

    /**
     * Set the fully qualified class-name of the connection factory that is used by this
     * builder to instantiate the connection factory
     *
     * @param connectionFactoryClass  fully qualified class-name
     */
    public void setConnectionFactoryClass(String connectionFactoryClass)
    {
      this.connectionFactoryClass = connectionFactoryClass;
    }

    @Override
    public ConnectionFactory buildConnectionFactory()
    {
      ConnectionFactory cf;
      try {
        if (connectionFactoryClass != null) {
          @SuppressWarnings("unchecked")
          Class<ConnectionFactory> clazz = (Class<ConnectionFactory>)Class.forName(connectionFactoryClass);
          cf = clazz.newInstance();
        } else {
          cf = new org.apache.activemq.ActiveMQConnectionFactory();
        }
        BeanUtils.populate(cf, connectionFactoryProperties);
        logger.debug("creation successful.");
        return cf;
      } catch (Exception e) {
        DTThrowable.rethrow(e);
        return null;  // previous rethrow makes this redundant, but compiler doesn't know...
      }
    }

    @Override
    public String toString()
    {
      return "DefaultConnectionFactoryBuilder [connectionFactoryProperties=" + connectionFactoryProperties + "]";
    }
  }

  /**
   * @return the connection
   */
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
   * @return the destination
   */
  public Destination getDestination()
  {
    return destination;
  }

  /**
   * gets the connection factory class-name used by the default connection factory builder
   *
   * @return connection factory class-name
   */
  public String getConnectionFactoryClass()
  {
    if (connectionFactoryBuilder == null) {
      connectionFactoryBuilder = createDefaultConnectionFactoryBuilderIfRequired();
    }
    if (connectionFactoryBuilder instanceof DefaultConnectionFactoryBuilder) {
      return ((DefaultConnectionFactoryBuilder)connectionFactoryBuilder).getConnectionFactoryClass();
    } else {
      throw new UnsupportedOperationException("ConnectionFactoryBuilder does not support connectionFactoryClass");
    }
  }

  /**
   * if the existing connectionFactoryBuilder is not of type DefaultConnectionFactoryBuilder
   * create one.
   *
   * @return the current DefaultConnectionFactoryBuilder value
   */
  private DefaultConnectionFactoryBuilder createDefaultConnectionFactoryBuilderIfRequired()
  {
    if (!(connectionFactoryBuilder instanceof DefaultConnectionFactoryBuilder)) {
      connectionFactoryBuilder = new DefaultConnectionFactoryBuilder();
    }
    return (DefaultConnectionFactoryBuilder)connectionFactoryBuilder;
  }

  /**
   * Sets the connection factory class-name used by the default connection factory builder
   *
   * @param connectionFactoryClass factory class-name to be set
   */
  public void setConnectionFactoryClass(String connectionFactoryClass)
  {
    DefaultConnectionFactoryBuilder builder =
        createDefaultConnectionFactoryBuilderIfRequired();
    builder.setConnectionFactoryClass(connectionFactoryClass);
  }

  /**
   * gets the connection factory builder of this instance
   *
   * @return connection factory builder
   */
  public ConnectionFactoryBuilder getConnectionFactoryBuilder()
  {
    return connectionFactoryBuilder;
  }

  /**
   * Sets the connection factory builder of this instance
   *
   * @param connectionFactoryBuilder connection factory builder for this instance
   */
  public void setConnectionFactoryBuilder(ConnectionFactoryBuilder connectionFactoryBuilder)
  {
    this.connectionFactoryBuilder = connectionFactoryBuilder;
  }

  /**
   * Return the connection factory properties. Property names are provider specific and can be set directly from configuration, for example:<p>
   * <code>dt.operator.JMSOper.connectionFactoryProperties.brokerURL=vm://localhost<code>
   * @return reference to mutable properties
   */
  public Map<String, String> getConnectionFactoryProperties()
  {
    if (connectionFactoryBuilder == null) {
      connectionFactoryBuilder = createDefaultConnectionFactoryBuilderIfRequired();
    }
    if (connectionFactoryBuilder instanceof DefaultConnectionFactoryBuilder) {
      return ((DefaultConnectionFactoryBuilder)connectionFactoryBuilder).getConnectionFactoryProperties();
    } else {
      throw new UnsupportedOperationException("ConnectionFactoryBuilder does not support connectionFactoryProperties");
    }
  }

  /**
   * Sets the connection factory properties. Property names are provider specific and can be set directly from configuration, for example:<p>
   * <code>dt.operator.JMSOper.connectionFactoryProperties.brokerURL=vm://localhost<code>
   *
   * @param connectionFactoryProperties reference to mutable properties
   */
  public void setConnectionFactoryProperties(Map<String, String> connectionFactoryProperties)
  {
    DefaultConnectionFactoryBuilder builder =
        createDefaultConnectionFactoryBuilderIfRequired();
    builder.setConnectionFactoryProperties(connectionFactoryProperties);
  }

  /**
   * @deprecated Use {@link #getConnectionFactoryProperties} to set properties supported by the connection factory.
   */
  @Deprecated
  public void setUser(String user)
  {
    this.getConnectionFactoryProperties().put("userName", user);
  }

  /**
   * @deprecated Use {@link #getConnectionFactoryProperties} to set properties supported by the connection factory.
   */
  @Deprecated
  public void setPassword(String password)
  {
    this.getConnectionFactoryProperties().put("password", password);
  }

  /**
   * @deprecated Use {@link #getConnectionFactoryProperties} to set properties supported by the connection factory.
   */
  @Deprecated
  public void setUrl(String url)
  {
    this.getConnectionFactoryProperties().put("brokerURL", url);
  }

  /**
   * @return the message acknowledgment mode
   */
  public String getAckMode()
  {
    return ackMode;
  }

  /**
   * Sets the message acknowledgment mode.
   *
   * @param ackMode the message acknowledgment mode to set
   */
  public void setAckMode(String ackMode)
  {
    this.ackMode = ackMode;
  }

  /**
   * @return the clientId
   */
  public String getClientId()
  {
    return clientId;
  }

  /**
   * Sets the client id.
   *
   * @param clientId the id to set for the client
   */
  public void setClientId(String clientId)
  {
    this.clientId = clientId;
  }

  /**
   * @return the name of the destination
   */
  public String getSubject()
  {
    return subject;
  }

  /**
   * Sets the name of the destination.
   *
   * @param subject the name of the destination to set
   */
  public void setSubject(String subject)
  {
    this.subject = subject;
  }

  /**
   * @return the batch
   */
  public int getBatch()
  {
    return batch;
  }

  /**
   * Sets the batch for the JMS operator. JMS can acknowledge receipt
   * of messages back to the broker in batches (to improve performance).
   *
   * @param batch the size of the batch
   */
  public void setBatch(int batch)
  {
    this.batch = batch;
  }

  /**
   * @return the message size
   */
  public int getMessageSize()
  {
    return messageSize;
  }

  /**
   * Sets the size of the message.
   *
   * @param messageSize the size of the message
   */
  public void setMessageSize(int messageSize)
  {
    this.messageSize = messageSize;
  }

  /**
   * @return the durability of the consumer
   */
  public boolean isDurable()
  {
    return durable;
  }

  /**
   * Sets the durability feature. Durable queues keep messages around persistently
   * for any suitable consumer to consume them.
   *
   * @param durable the flag to set to the durability feature
   */
  public void setDurable(boolean durable)
  {
    this.durable = durable;
  }

  /**
   * @return the topic
   */
  public boolean isTopic()
  {
    return topic;
  }

  /**
   * Sets of the destination is a topic or a queue.
   *
   * @param topic the flag to set the destination to topic or queue.
   */
  public void setTopic(boolean topic)
  {
    this.topic = topic;
  }

  public boolean isVerbose()
  {
    return verbose;
  }

  /**
   * Sets the verbose option.
   *
   * @param verbose the flag to set to enable verbose option
   */
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
    } else if ("AUTO_ACKNOWLEDGE".equals(ackMode)) {
      return Session.AUTO_ACKNOWLEDGE;
    } else if ("DUPS_OK_ACKNOWLEDGE".equals(ackMode)) {
      return Session.DUPS_OK_ACKNOWLEDGE;
    } else if ("SESSION_TRANSACTED".equals(ackMode)) {
      return Session.SESSION_TRANSACTED;
    } else {
      return Session.CLIENT_ACKNOWLEDGE; // default
    }
  }

  /**
   *  Connection specific setup for JMS.
   *
   *  @throws JMSException
   */
  public void createConnection() throws JMSException
  {

    connection = getConnectionFactory().createConnection();
    if (durable && clientId != null) {
      connection.setClientID(clientId);
    }

    logger.debug("Before starting connection.");
    connection.start();
    logger.debug("After starting connection.");

    // Create session
    session = connection.createSession(transacted, getSessionAckMode(ackMode));

    // Create destination
    destination = topic ? session.createTopic(subject) : session.createQueue(subject);
  }

  /**
   * Implement connection factory lookup.
   */
  protected ConnectionFactory getConnectionFactory()
  {
    logger.debug("connectionFactoryBuilder {}", "" + connectionFactoryBuilder);

    return connectionFactoryBuilder.buildConnectionFactory();

  }

  /**
   *  cleanup connection resources.
   */
  protected void cleanup()
  {
    try {
      session.close();
      connection.close();
      session = null;
      connection = null;
    } catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   * @return the transacted
   */
  public boolean isTransacted()
  {
    return transacted;
  }
}
