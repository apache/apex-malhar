/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.kafka;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import kafka.message.Message;


/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class KafkaSinglePortInputOperator<T> extends KafkaInputOperator
{
  /**
   * The single output port.
   */
  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

  /**
   * Any concrete class derived from KafkaSinglePortInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   * It converts a ByteBuffer message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param msg
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
