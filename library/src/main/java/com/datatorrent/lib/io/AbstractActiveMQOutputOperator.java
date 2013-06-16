/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Operator;
import javax.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ActiveMQ output adapter operator, which produce sends into ActiveMQ message bus.<p><br>
 *
 * This operator receives tuples from Malhar Streaming Platform through its input ports.
 * When the tuple is available in input ports it converts that to JMS message and send into
 * AMQ message bus. The concrete class of this has to implement the abstract method
 * how to convert tuple into JMS message.
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * @author Locknath Shil <locknath@malhar-inc.com>
 *
 */
public abstract class AbstractActiveMQOutputOperator extends ActiveMQProducerBase implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQOutputOperator.class);
  long maxSendMessage = 0; // max send limit

  /**
   * Implement Component Interface.
   *
   * @param context
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
