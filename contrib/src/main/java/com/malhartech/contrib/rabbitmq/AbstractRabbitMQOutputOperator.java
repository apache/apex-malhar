/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.rabbitmq;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.OperatorConfiguration;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class AbstractRabbitMQOutputOperator<T> extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractRabbitMQInputOperator.class);
  ConnectionFactory connFactory = new ConnectionFactory();
  QueueingConsumer consumer = null;
  Connection connection = null;
  Channel channel = null;
  final String exchange = "test";
  public String queueName;

  @Override
  public void setup(OperatorConfiguration config)
  {
    try {
      connFactory.setHost("localhost");
      connection = connFactory.newConnection();
      channel = connection.createChannel();
    }
    catch (IOException ex) {
      logger.debug(ex.toString());
    }
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  @Override
  public void teardown()
  {
    try {
      channel.close();
      connection.close();
    }
    catch (IOException ex) {
      logger.debug(ex.toString());
    }
  }
}
