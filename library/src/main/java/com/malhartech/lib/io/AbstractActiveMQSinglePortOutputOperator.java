/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class AbstractActiveMQSinglePortOutputOperator<T> extends AbstractActiveMQOutputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQSinglePortOutputOperator.class);
  long countMessages = 0;  // Number of message produced so far

  /**
   * Define input ports as needed.
   */
  @InputPortFieldAnnotation(name = "ActiveMQInputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T t)
    {
      logger.debug("process got called from {}", this);
      countMessages++;
      // Stop sending messages after max limit.
      if (countMessages > maxSendMessage && maxSendMessage != 0) {
        if (countMessages == maxSendMessage + 1) {
          logger.warn("Reached maximum send messages of {}", maxSendMessage);
        }
        return;
      }

      try {
        Message msg = createMessage(t);
        getProducer().send(msg);

        logger.debug("process got called from {} with message {}", this, t.toString());
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
    }
  };
}
