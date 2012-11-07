/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Operator;
import javax.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 *
 * This is ActiveMQ output adapter operator (which produce data into ActiveMQ message bus).
 */
public abstract class AbstractActiveMQOutputOperator extends ActiveMQProducerBase implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQOutputOperator.class);
  long maxSendMessage = 0; // max send limit

  /**
   * Implement Component Interface.
   *
   * @param config
   */
  @Override
  public void setup(OperatorContext context)
  {
    try {
      setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    maxSendMessage = getMaximumSendMessages();
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    cleanup();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }
}
