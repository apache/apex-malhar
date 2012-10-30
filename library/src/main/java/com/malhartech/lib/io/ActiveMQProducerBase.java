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
public class ActiveMQProducerBase extends ActiveMQBase
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQProducerBase.class);
  private MessageProducer producer;
  private long maximumSendMessages = 0; // 0 means unlimitted

  public long getMaximumSendMessages()
  {
    return maximumSendMessages;
  }

  public void setMaximumSendMessages(long maximumSendMessages)
  {
    this.maximumSendMessages = maximumSendMessages;
  }

  public MessageProducer getProducer()
  {
    return producer;
  }

  /**
   * Connection specific setup for ActiveMQ.
   *
   * @throws JMSException
   */
  public void setupConnection() throws JMSException
  {
    super.createConnection();
    // Create producer
    producer = session.createProducer(destination);
    //producer.setDeliveryMode(DeliveryMode.PERSISTENT);
    //maxMessages = super.getMaximumMessage();
  }

  /**
   * Release resources.
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
