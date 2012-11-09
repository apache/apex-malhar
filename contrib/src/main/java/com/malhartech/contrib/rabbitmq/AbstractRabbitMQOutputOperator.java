/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.rabbitmq;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RabbitMQ output adapter operator, which send data to RabbitMQ message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>host</b>:the address for the consumer to connect <br>
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
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class AbstractRabbitMQOutputOperator<T> extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractRabbitMQInputOperator.class);
  transient ConnectionFactory connFactory = new ConnectionFactory();
  transient QueueingConsumer consumer = null;
  transient Connection connection = null;
  transient Channel channel = null;
  transient final String exchange = "test";
  transient public String queueName="testQ";

  @Override
  public void setup(OperatorContext context)
  {
    try {
      connFactory.setHost("localhost");
      connection = connFactory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare(queueName, false, false, false, null);
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
