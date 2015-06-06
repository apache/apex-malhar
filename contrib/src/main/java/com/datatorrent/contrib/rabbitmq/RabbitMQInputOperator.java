package com.datatorrent.contrib.rabbitmq;

/**
 * Input adapter operator which consumes byte array data from the RabbitMQ and outputs byte array.
 * <p></p>
 *
 * @displayName Rabbit MQ input operator
 * @category Messaging
 * @tags input operator, string
 *
 */
public class RabbitMQInputOperator extends AbstractSinglePortRabbitMQInputOperator<byte[]>
{
  @Override
  public byte[] getTuple(byte[] message)
  {
    return message;
  }
}
