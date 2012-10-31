/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import javax.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class ActiveMQConsumerBase extends ActiveMQBase implements MessageListener, ExceptionListener
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQConsumerBase.class);
  private MessageProducer replyProducer;
  private MessageConsumer consumer;
  private long messagesReceived = 0;
  private String consumerName;
 private long maxMessages;


   /**
   * Any ActiveMQINputOperator has to implement this method
   * so that it knows how to emit message to what port.
   *
   * @param message
   */
  protected abstract void emitMessage(Message message) throws JMSException;


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

  public long getMessagesReceived()
  {
    return messagesReceived;
  }

  public void setMessagesReceived(long messagesReceived)
  {
    this.messagesReceived = messagesReceived;
  }

  public String getConsumerName()
  {
    return consumerName;
  }

  public void setConsumerName(String consumerName)
  {
    this.consumerName = consumerName;
  }

  /**
   * Connection specific setup for ActiveMQ.
   *
   * @throws JMSException
   */
  public void setupConnection() throws JMSException
  {
    super.createConnection();
    replyProducer = session.createProducer(null);

    consumer = (durable && topic)
               ? session.createDurableSubscriber((Topic)destination, consumerName)
               : session.createConsumer(destination);
    consumer.setMessageListener(this);
    maxMessages = super.getMaximumMessage();
  }

  /**
   * Commit/Acknowledge message that has been received.
   * @param message
   */
  public void acknowledgeMessage(Message message)
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
   * Whenever there is message available this will get called.
   * This just emit the message to Malhar platform.
   *
   * @param message
   */
  @Override
  public void onMessage(Message message)
  {
    // Make sure that we do not get called again if we have processed enough messages already.
    logger.debug("onMessage got called from {}", this);
    if (maxMessages > 0) {
      if (--maxMessages == 0) {
        try {
          consumer.setMessageListener(null);
        }
        catch (JMSException ex) {
          logger.error(ex.getLocalizedMessage());
        }
      }
    }

    try {
      //super.outputPort.emit(getTuple(message));
      emitMessage(message);
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }


    acknowledgeMessage(message);
  }

  @Override
  public void onException(JMSException ex)
  {
    logger.error(ex.getLocalizedMessage());
  }

  /**
   * Release resources.
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
