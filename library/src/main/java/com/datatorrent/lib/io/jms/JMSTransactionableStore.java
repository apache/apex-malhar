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
package com.datatorrent.lib.io.jms;

import java.io.IOException;
import java.util.Enumeration;
import javax.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transactionable store commits the messages sent within a window along with the windowId of the completed window
 * to JMS. This store ensures that the JMS output operator is capable of outputting data to JMS exactly once.
 *
 * @since 2.0.0
 */
public class JMSTransactionableStore extends JMSBaseTransactionableStore
{
  private static final Logger logger = LoggerFactory.getLogger(JMSTransactionableStore.class);

  private transient MessageProducer producer;
  private transient MessageConsumer consumer;

  /**
   * Indicates whether the store is connected or not.
   */
  private transient boolean connected = false;
  /**
   * Indicates whether the store is in a transaction or not.
   */
  private transient boolean inTransaction = false;

  public JMSTransactionableStore()
  {
  }

  @Override
  @SuppressWarnings("rawtypes")
  public long getCommittedWindowId(String appId, int operatorId)
  {
    logger.debug("Getting committed windowId appId {} operatorId {}", appId, operatorId);

    try {

      beginTransaction();
      BytesMessage message = (BytesMessage) consumer.receive();
      logger.debug("Retrieved committed window message id {}", message.getJMSMessageID());
      long windowId = message.readLong();

      message = getBase().getSession().createBytesMessage();
      message.writeLong(windowId);
      producer.send(message);
      commitTransaction();

      logger.debug("Retrieved windowId {}", windowId);
      return windowId;
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    if(!inTransaction) {
      throw new RuntimeException("This should be called while you are in an existing transaction");
    }

    logger.debug("storing window appId {} operatorId {} windowId {}",
                 appId, operatorId, windowId);
    try {
      removeCommittedWindowId(appId, operatorId);
      BytesMessage bytesMessage = this.getBase().getSession().createBytesMessage();
      bytesMessage.writeLong(windowId);
      producer.send(bytesMessage);
      logger.debug("Retrieved committed window message id {}", bytesMessage.getJMSMessageID());
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
    try {
      consumer.receive();
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginTransaction()
  {
    logger.debug("beginning transaction");

    if(inTransaction)
    {
      throw new RuntimeException("Cannot start a transaction twice.");
    }

    inTransaction = true;
  }

  @Override
  public void commitTransaction()
  {
    logger.debug("committing transaction.");

    if(!inTransaction)
    {
      throw new RuntimeException("Cannot commit a transaction if you are not in one.");
    }

    try {
      getBase().getSession().commit();
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    inTransaction = false;
    logger.debug("finished committing transaction.");
  }

  @Override
  public void rollbackTransaction()
  {
    try
    {
      getBase().getSession().rollback();
    }
    catch (JMSException ex)
    {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean isInTransaction()
  {
    return inTransaction;
  }

  @Override
  public void connect() throws IOException
  {
    logger.debug("Entering connect. is in transaction: {}", inTransaction);

    try {
      String queueName = getQueueName(getAppId(),
                                      getOperatorId());

      logger.debug("Base is null: {}", getBase() == null);

      if(getBase() != null) {
        logger.debug("Session is null: {}", getBase().getSession() == null);
      }

      Queue queue = getBase().getSession().createQueue(queueName);
      QueueBrowser browser = getBase().getSession().createBrowser(queue);
      boolean hasStore;

      try {
        Enumeration enumeration = browser.getEnumeration();
        hasStore = enumeration.hasMoreElements();
      }
      catch (JMSException ex) {
        throw new RuntimeException(ex);
      }

      producer = getBase().getSession().createProducer(queue);
      consumer = getBase().getSession().createConsumer(queue);

      connected = true;
      logger.debug("Connected. is in transaction: {}", inTransaction);

      if(!hasStore) {
        beginTransaction();
        BytesMessage message = getBase().getSession().createBytesMessage();
        message.writeLong(-1L);
        producer.send(message);
        commitTransaction();
      }
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    logger.debug("Exiting connect. is in transaction: {}", inTransaction);
  }

  @Override
  public void disconnect() throws IOException
  {
    logger.debug("disconnectiong");
    try {
      producer.close();
      consumer.close();
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    inTransaction = false;
    connected = false;
    logger.debug("done disconnectiong");
  }

  @Override
  public boolean isConnected()
  {
    return connected;
  }

  private String getQueueName(String appId, int operatorId)
  {
    return appId + "-" + operatorId;
  }
}
