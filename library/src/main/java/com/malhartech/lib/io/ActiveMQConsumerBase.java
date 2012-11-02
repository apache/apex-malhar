/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InjectConfig;
import javax.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *  @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class ActiveMQConsumerBase extends ActiveMQBase implements MessageListener, ExceptionListener
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQConsumerBase.class);
  private transient MessageProducer replyProducer;
  private transient MessageConsumer consumer;
  private long messageReceivedCount = 0;

  // Config parameters that user can set.
  @InjectConfig(key = "consumerName")
  private String consumerName;
  @InjectConfig(key = "maximumReceiveMessages")
  private long maximumReceiveMessages = 0; // 0 means unlimitted, can be set by user

  /**
   *  Any ActiveMQINputOperator has to implement this method
   *  so that it knows how to emit message to what port.
   *
   *  @param message
   */
  protected abstract void emitMessage(Message message) throws JMSException;

  public MessageProducer getReplyProducer()
  {
    return replyProducer;
  }

  public MessageConsumer getConsumer()
  {
    return consumer;
  }

  public long getMessagesReceived()
  {
    return messageReceivedCount;
  }

  public void setMessagesReceived(long messagesReceived)
  {
    this.messageReceivedCount = messagesReceived;
  }

  public String getConsumerName()
  {
    return consumerName;
  }

  public void setConsumerName(String consumerName)
  {
    this.consumerName = consumerName;
  }

  public long getMaximumReceiveMessages()
  {
    return maximumReceiveMessages;
  }

  public void setMaximumReceiveMessages(long maximumReceiveMessages)
  {
    this.maximumReceiveMessages = maximumReceiveMessages;
  }

  /**
   *  Connection specific setup for ActiveMQ.
   *
   *  @throws JMSException
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
   *  Commit/Acknowledge message that has been received.
   *
   *  @param message
   */
  public void acknowledgeMessage(Message message)
  {
    try {
      if (message.getJMSReplyTo() != null) {
        // Send reply only if the replyTo destination is set
        replyProducer.send(message.getJMSReplyTo(), getSession().createTextMessage("Reply: " + message.getJMSMessageID()));
      }

      if (isTransacted()) {
        if ((messageReceivedCount % getBatch()) == 0) {
          if (isVerbose()) {
            System.out.println("Commiting transaction for last " + getBatch() + " messages; messages so far = " + messageReceivedCount);
          }
          getSession().commit();
        }
      }
      else if (getSessionAckMode(getAckMode()) == Session.CLIENT_ACKNOWLEDGE) {
        // we can use window boundary to ack the message.
        if ((messageReceivedCount % getBatch()) == 0) {
          if (isVerbose()) {
            System.out.println("Acknowledging last " + getBatch() + " messages; messages so far = " + messageReceivedCount);
          }
          message.acknowledge(); // acknowledge all consumed messages upto now
        }
      }
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   *  Implement MessageListener interface.
   *
   *  Whenever there is message available in ActiveMQ message bus this will get called.
   *  This just emit the message to Malhar platform.
   *
   *  @param message
   */
  @Override
  public void onMessage(Message message)
  {
    ++messageReceivedCount;

    // Make sure that we do not get called again if we have processed enough messages already.
    logger.debug("onMessage got called from {} with {}", this, messageReceivedCount);

    if (messageReceivedCount == maximumReceiveMessages) {
      try {
        consumer.setMessageListener(null);
      }
      catch (JMSException ex) {
        logger.error(ex.getLocalizedMessage());
      }
    }

    try {
      emitMessage(message); // Call abstract method to send message to ActiveMQ input operator.
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }

    acknowledgeMessage(message);
  }

  /**
   *  Implement ExceptionListener interface.
   *
   *  @param ex
   */
  @Override
  public void onException(JMSException ex)
  {
    logger.error(ex.getLocalizedMessage());
  }

  /**
   *  Release resources.
   */
  @Override
  public void cleanup()
  {
    try {
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
