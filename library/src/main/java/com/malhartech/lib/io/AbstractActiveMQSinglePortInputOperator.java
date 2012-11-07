/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import java.util.logging.Level;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 *
 * This Input Operator has only one output port.
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
   * @param message
   */
  public abstract T getTuple(Message msg);

  /**
   * Implement abstract method.
   */
  @Override
  public void emitTuple(Message msg)
  {
    outputPort.emit(getTuple(msg));
  }
}
