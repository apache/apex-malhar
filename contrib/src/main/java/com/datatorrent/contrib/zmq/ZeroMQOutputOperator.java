package com.datatorrent.contrib.zmq;

/**
 * Output adapter operator with a single input port which consumes byte array and emits the same to the ZeroMQ port.
 * <p></p>
 *
 * @displayName Single Port Zero MQ output operator
 * @category Messaging
 * @tags input operator, string
 *
 */

public class ZeroMQOutputOperator extends AbstractSinglePortZeroMQOutputOperator<byte[]>{

	@Override
	public void processTuple(byte[] tuple) {
		publisher.send(tuple, 0);
	}
}
