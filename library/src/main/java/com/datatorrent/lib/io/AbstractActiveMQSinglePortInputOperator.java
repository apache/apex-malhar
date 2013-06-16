/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import javax.jms.Message;

/**
 * ActiveMQ input adapter operator with single output port, which consume data from ActiveMQ message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Have only one output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method getTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 *
 * @param <T>
 * @author Locknath Shil <locknath@malhar-inc.com>
 *
 */
public abstract class AbstractActiveMQSinglePortInputOperator<T> extends AbstractActiveMQInputOperator
{
  /**
   * The single output port.
   */
  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

  /**
   * Any concrete class derived from AbstractActiveMQSinglePortInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   * It converts a JMS message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param msg
   * @return newly constructed tuple from the message.
   */
  public abstract T getTuple(Message msg);

  /**
   * Implement abstract method.
   * @param msg
   */
  @Override
  public void emitTuple(Message msg)
  {
    T payload = getTuple(msg);
    if (payload != null) {
      outputPort.emit(payload);
    }
  }
}
