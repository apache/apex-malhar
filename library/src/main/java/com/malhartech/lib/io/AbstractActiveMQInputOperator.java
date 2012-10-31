/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.InputOperator;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.util.CircularBuffer;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 * This is ActiveMQ input adapter operator (which consume data from ActiveMQ message bus).
 */
public abstract class AbstractActiveMQInputOperator<T> extends BaseOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQInputOperator.class);

  private final int bufferSize = 1024*1024;
  private transient CircularBuffer<T> holdingBuffer = new CircularBuffer<T>(bufferSize);

  public AbstractActiveMQInputOperator()
  {
  }

  private ActiveMQConsumerBase amqConsumer = new ActiveMQConsumerBase()
  {
    @Override
    protected void emitMessage(Message message) throws JMSException
    {
      // super.outputPort.emit(getTuple(message));
      T tuple = getTuple(message);
      holdingBuffer.add(tuple);
    }
  };

  //protected abstract void emitTuple(T t) throws JMSException;

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

  /**
   * Implement Component Interface.
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config)
  {
    try {
      logger.debug("setup got called from {}", this);
      amqConsumer.setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    logger.debug("teardown got called from {}", this);
    amqConsumer.cleanup();
  }
}
