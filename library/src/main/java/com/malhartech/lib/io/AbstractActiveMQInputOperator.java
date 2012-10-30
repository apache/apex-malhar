/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.InputOperator;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;
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
public abstract class AbstractActiveMQInputOperator<T> extends BaseOperator implements InputOperator, Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQInputOperator.class);
 // private long maxMessages;

  private ActiveMQConsumerBase amqConsumer = new ActiveMQConsumerBase() {

    @Override
    protected void emitMessage(Message message) throws JMSException
    {
     // super.outputPort.emit(getTuple(message));
      emitTuple(getTuple(message));
    }
  };

  protected abstract void emitTuple(T t) throws JMSException;

  public AbstractActiveMQInputOperator()
  {
  }

  public ActiveMQConsumerBase getAmqConsumer()
  {
    return amqConsumer;
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
      logger.debug("setup got called from {}", this);
      amqConsumer.setupConnection();
      //amqConfig.getConsumer().setMessageListener(this);

      //maxMessages = amqConsumer.getMaximumMessage();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }

  }

  @Override
  public void teardown()
  {
    logger.debug("teardown got called from {}", this);
    amqConsumer.cleanup();
  }

  /**
   * Whenever there is message available this will get called.
   * This just emit the message to Malhar platform.
   *
   * @param message
   */
/*  @Override
  public void onMessage(Message message)
  {
    // Make sure that we do not get called again if we have processed enough messages already.
    logger.debug("onMessage got called from {}", this);
    if (maxMessages > 0) {
      if (--maxMessages == 0) {
        try {
          amqConsumer.getConsumer().setMessageListener(null);
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


    amqConsumer.acknowledgeMessage(message);
  }

  @Override
  public void onException(JMSException ex)
  {
    logger.error(ex.getLocalizedMessage());
  } */
}
