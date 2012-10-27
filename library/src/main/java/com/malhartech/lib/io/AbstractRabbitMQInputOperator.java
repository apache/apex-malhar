/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InjectConfig;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.*;
import com.malhartech.dag.OperatorContext;
import com.malhartech.util.CircularBuffer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractRabbitMQInputOperator<T> extends BaseOperator implements AsyncInputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractRabbitMQInputOperator.class);
  private volatile boolean running = false;
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

  CircularBuffer<byte[]> tempBuffer = new CircularBuffer<byte[]>(1024 * 1024);
  @InjectConfig(key = "host")
  private String host;
  @InjectConfig(key = "exchange")
  private String exchange;
  ConnectionFactory connFactory = new ConnectionFactory();
  QueueingConsumer consumer = null;
  Connection connection = null;
  Channel channel = null;

  @NotNull
  public void setHost(String host)
  {
    this.host = host;
  }

  @NotNull
  public void setExchange(String exchange)
  {
    this.exchange = exchange;
  }

  @Override
  public void setup(OperatorConfiguration config)
  {
    try {
      super.setup(config);
      connFactory.setHost(host);
      connection = connFactory.newConnection();
      channel = connection.createChannel();
      channel.exchangeDeclare(exchange, "fanout");
      String queueName = channel.queueDeclare().getQueue();
      
      channel.queueBind(queueName, exchange, "");
      consumer = new QueueingConsumer(channel);
      channel.basicConsume(queueName, true, consumer);
    }
    catch (IOException ex) {
      logger.debug(ex.toString());
    }
  }

  public void postActivate(OperatorContext ctx)
  {
    new Thread()
    {
      @Override
      public void run()
      {
        running = true;
        while (running) {
          try {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            tempBuffer.add(delivery.getBody());
            logger.debug(" [x] Received Message:" + message);
          }
          catch (Exception e) {
//        logger.debug(e.toString());
            break;
          }
        }
      }
    }.start();
  }

  public abstract T getOutputTuple(byte[] message);

  @Override
  public void emitTuples(long windowId)
  {
    for (int i = tempBuffer.size(); i-- > 0;) {
      outputPort.emit(getOutputTuple(tempBuffer.pollUnsafe()));
    }
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

  public void preDeactivate()
  {
    running = false;
  }
}
