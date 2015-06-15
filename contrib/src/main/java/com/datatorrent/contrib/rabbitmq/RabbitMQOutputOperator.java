package com.datatorrent.contrib.rabbitmq;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.DTThrowable;

/**
 * Output adapter operator which consumes byte array and emits the same to the RabbitMQ.
 * <p></p>
 *
 * @displayName Rabbit MQ output operator
 * @category Messaging
 * @tags input operator, string
 *
 */
public class RabbitMQOutputOperator extends AbstractSinglePortRabbitMQOutputOperator<byte[]>
{
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQOutputOperator.class);
  
  @Override
  public void processTuple(byte[] tuple)
  {
    try {
      channel.basicPublish(exchange, "", null, tuple);
    } catch (IOException e) {

      logger.debug(e.toString());
      DTThrowable.rethrow(e);
    }   
  }
}
