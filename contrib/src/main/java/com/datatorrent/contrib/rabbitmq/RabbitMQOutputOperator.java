package com.datatorrent.contrib.rabbitmq;

import java.io.IOException;

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
  @Override
  public void processTuple(byte[] tuple)
  {
      try {
        channel.basicPublish(exchange, "", null, tuple);
      } catch (IOException e) {
        
        logger.debug(e.toString());
      }   
  }
}