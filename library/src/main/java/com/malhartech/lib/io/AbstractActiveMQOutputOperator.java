/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.OperatorConfiguration;
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
  public ActiveMQBase amqConfig;
  long maximumSendMessages = 0;
  long countMessages = 0; // Number of message produced

  protected abstract Message createMessage(T obj);

  public AbstractActiveMQOutputOperator(ActiveMQBase config)
  {
    amqConfig = config;
  }

  /**
   * Do connection setup with activeMQ service.
   *
   * @param config
   * @throws FailedOperationException
   */
  @Override
  public void setup(OperatorConfiguration config)
  {
    try {
      //System.out.println("setup got called");
      amqConfig.setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    maximumSendMessages = amqConfig.getMaximumSendMessages();
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
      if (countMessages++ >= maximumSendMessages && maximumSendMessages != 0) {
        if (countMessages == maximumSendMessages) {
          logger.warn("Reached maximum send messages of {}", maximumSendMessages);
        }
        return;
      }

      try {
        Message msg = createMessage(t);
        amqConfig.getProducer().send(msg);
        System.out.println(String.format("Called process() with message %s", t.toString()));
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
    //System.out.println("cleanup got called");
    amqConfig.cleanup();
  }
}
