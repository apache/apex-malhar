/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for any ActiveMQ output adapter operator. <p><br>
 * Output Operators should not be derived from this,
 * rather from AbstractActiveMQOutputOperator or AbstractActiveMQSinglePortOutputOperator. It creates the producer
 * to send message into active MQ message bus.<br>
 *
 * <br>
 * Ports:<br>
 * None<br>
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
 * NA<br>
 * <br>
 * @author Locknath Shil <locknath@malhar-inc.com>
 *
 */
public class ActiveMQProducerBase extends ActiveMQBase
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQProducerBase.class);
  private transient MessageProducer producer;

  // Config parameters that user can set.
  private long maximumSendMessages = 0; // 0 means unlimitted

  public MessageProducer getProducer()
  {
    return producer;
  }

  public long getMaximumSendMessages()
  {
    return maximumSendMessages;
  }

  public void setMaximumSendMessages(long maximumSendMessages)
  {
    this.maximumSendMessages = maximumSendMessages;
  }

  /**
   *  Connection specific setup for ActiveMQ.
   *
   *  @throws JMSException
   */
  public void setupConnection() throws JMSException
  {
    super.createConnection();
    // Create producer
    producer = getSession().createProducer(getDestination());
  }

  /**
   *  Release resources.
   */
  @Override
  public void cleanup()
  {
    try {
      producer.close();
      producer = null;

      super.cleanup();
    }
    catch (JMSException ex) {
      logger.error(null, ex);
    }
  }
}
