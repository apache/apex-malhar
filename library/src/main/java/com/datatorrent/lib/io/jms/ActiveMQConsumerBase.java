/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.jms;

import javax.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for any ActiveMQ input adapter operator. <p><br>
 * Input Operators should not be derived from this,
 * rather from AbstractActiveMQInputOperator or AbstractActiveMQSinglePortInputOperator. This consumes message
 * from active MQ message bus through onMessage() call.<br>
 *
 * <br>
 * Ports:<br>
 * None<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitMessage() <br>
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
public abstract class ActiveMQConsumerBase extends ActiveMQBase implements MessageListener, ExceptionListener
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQConsumerBase.class);
  private transient MessageProducer replyProducer;
  private transient MessageConsumer consumer;
  private long messageReceivedCount = 0;
  // Config parameters that user can set.
  private String consumerName;
  private long maximumReceiveMessages = 0; // 0 means unlimitted, can be set by user

  /**
   * Any ActiveMQINputOperator has to implement this method
   * so that it knows how to emit message to what port.
   *
   * @param message
   */
  protected abstract void emitMessage(Message message) throws JMSException;

  /**
   * @return the message producer 
   */
  public MessageProducer getReplyProducer()
  {
    return replyProducer;
  }

  /**
   * @return the message consumer 
   */
  public MessageConsumer getConsumer()
  {
    return consumer;
  }

  /**
   * @return the count of messages received
   */
  public long getMessagesReceived()
  {
    return messageReceivedCount;
  }


  /**
   * Sets the number of received messages.
   * 
   * @param messagesReceived the number of received messages
   */
  public void setMessagesReceived(long messagesReceived)
  {
    this.messageReceivedCount = messagesReceived;
  }

  /**
   * @return the consumer name 
   */
  public String getConsumerName()
  {
    return consumerName;
  }

  /**
   * Sets the name for the consumer.
   * 
   * @param consumerName- the name for the consumer
   */
  public void setConsumerName(String consumerName)
  {
    this.consumerName = consumerName;
  }

  /**
   * @return the maximum of received messages 
   */
  public long getMaximumReceiveMessages()
  {
    return maximumReceiveMessages;
  }

  /**
   * Sets the maximum number of messages that can be received.
   * 
   * @param maximumReceiveMessages the maximum number of received messages
   */
  public void setMaximumReceiveMessages(long maximumReceiveMessages)
  {
    this.maximumReceiveMessages = maximumReceiveMessages;
  }

  /**
   * Connection specific setup for ActiveMQ.
   *
   * @throws JMSException
   */
  public void setupConnection() throws JMSException
  {
    super.createConnection();
    replyProducer = getSession().createProducer(null);

    consumer = (isDurable() && isTopic())
               ? getSession().createDurableSubscriber((Topic)getDestination(), consumerName)
               : getSession().createConsumer(getDestination());
    consumer.setMessageListener(this);
  }

  /**
   * If getJMSReplyTo is set then send message back to reply producer.
   *
   * @param message
   */
  public void sendReply(Message message)
  {
    try {
      if (message.getJMSReplyTo() != null) { // Send reply only if the replyTo destination is set
        replyProducer.send(message.getJMSReplyTo(), getSession().createTextMessage("Reply: " + message.getJMSMessageID()));
      }
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   * Commit/Acknowledge message that has been received.
   *
   * @param message
   */
  public void acknowledgeMessage(Message message)
  {
    try {
      if (isTransacted()) {
        getSession().commit();
      }
      else if (getSessionAckMode(getAckMode()) == Session.CLIENT_ACKNOWLEDGE) {
        message.acknowledge(); // acknowledge all consumed messages upto now
      }
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   * Implement MessageListener interface.
   *
   * Whenever there is message available in ActiveMQ message bus this will get called.
   * This just emit the message to Malhar platform.
   *
   * @param message
   */
  @Override
  public void onMessage(Message message)
  {
    ++messageReceivedCount;

    //logger.debug("onMessage got called from {} with {}", this, messageReceivedCount);
    try {
      if (messageReceivedCount == maximumReceiveMessages) {
        consumer.setMessageListener(null); // Make sure that we do not get called again if we have processed enough messages already.
      }
      emitMessage(message); // Call abstract method to send message to ActiveMQ input operator.
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }

    sendReply(message);
  }

  /**
   * Implement ExceptionListener interface.
   *
   * @param ex
   */
  @Override
  public void onException(JMSException ex)
  {
    cleanup();
    logger.error(ex.getLocalizedMessage());
  }

  /**
   * Release resources.
   */
  @Override
  public void cleanup()
  {
    try {
      consumer.setMessageListener(null);
      replyProducer.close();
      replyProducer = null;
      consumer.close();
      consumer = null;

      super.cleanup();
    }
    catch (JMSException ex) {
      logger.error(null, ex);
    }
  }
}
