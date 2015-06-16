package com.datatorrent.contrib.zmq;

/**
 * Input adapter operator with a single output port, which consumes byte array data from the ZeroMQ and outputs byte array.
 * <p></p>
 *
 * @displayName Single Port Zero MQ input operator
 * @category Messaging
 * @tags input operator, string
 *
 */
public class ZeroMQInputOperator extends AbstractSinglePortZeroMQInputOperator<byte[]>
{
  @Override
  public byte[] getTuple(byte[] message) {  	
    return message;
  }
}
