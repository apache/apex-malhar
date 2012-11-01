/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 *
 * This is ActiveMQ output adapter operator (which produce data into ActiveMQ message bus).
 */
public abstract class AbstractActiveMQOutputOperator<T> extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQOutputOperator.class);
  protected ActiveMQProducerBase amqProducer = new ActiveMQProducerBase();
  long maximumSendMessages = 0;
  long countMessages = 0; // Number of message produced

  protected abstract Message createMessage(T obj);

  public AbstractActiveMQOutputOperator()
  {
  }

  public ActiveMQProducerBase getAmqProducer()
  {
    return amqProducer;
  }

  /**
   * Do connection setup with activeMQ service.
   *
   * @param config
   */
  @Override
  public void setup(OperatorContext context)
  {
    try {
      logger.debug("setup got called from {}", this);
      amqProducer.setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    maximumSendMessages = amqProducer.getMaximumSendMessages();
  }

  /**
   * Implements Sink interface.
   */
  @InputPortFieldAnnotation(name = "ActiveMQInputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T t)
    {
      logger.debug("process got called from {}", this);

      if (countMessages++ >= maximumSendMessages && maximumSendMessages != 0) {
        if (countMessages == maximumSendMessages) {
          logger.warn("Reached maximum send messages of {}", maximumSendMessages);
        }
        return;
      }

      try {
        Message msg = createMessage(t);
        amqProducer.getProducer().send(msg);

        logger.debug("process got called from {} with message {}", this, t.toString());
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
    }
  };

  /**
   * Close connection attributes.
   */
  @Override
  public void teardown()
  {
    logger.debug("teardown got called from {}", this);
    amqProducer.cleanup();
  }
}
