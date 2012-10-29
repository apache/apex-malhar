/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.BaseInputOperator;
import com.malhartech.api.OperatorConfiguration;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 * This is ActiveMQ input adapter operator (which consume data from ActiveMQ message bus).
 */
public abstract class AbstractActiveMQInputOperator<T> extends BaseInputOperator<T> implements MessageListener, ExceptionListener
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQInputOperator.class);
  private long maxMessages;
  public ActiveMQBase amqConfig;

  public AbstractActiveMQInputOperator(ActiveMQBase config)
  {
    amqConfig = config;
  }

  /**
   * Any concrete class derived from AbstractActiveQConsumerModule has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   *
   * @param message
   */
  protected abstract T getTuple(Message message) throws JMSException;

  @Override
  public void setup(OperatorConfiguration config)
  {
    try {
      //logger.debug("setup got called");
      amqConfig.setupConnection();
      amqConfig.getConsumer().setMessageListener(this);

      maxMessages = amqConfig.getMaximumMessage();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }

  }

  @Override
  public void teardown()
  {
    logger.debug("teardown got called from {}", this);
    amqConfig.cleanup();
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
    /**
     * make sure that we do not get called again if we have processed enough
     * messages already.
     */
    //logger.debug("{} in call onMessage", this);
    if (maxMessages > 0) {
      if (--maxMessages == 0) {
        try {
          amqConfig.getConsumer().setMessageListener(null);
        }
        catch (JMSException ex) {
          logger.error(ex.getLocalizedMessage());
        }
      }
    }

    try {
      super.outputPort.emit(getTuple(message));
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }


    amqConfig.handleMessage(message);
  }

  @Override
  public void onException(JMSException ex)
  {
    logger.error(ex.getLocalizedMessage());
  }
}