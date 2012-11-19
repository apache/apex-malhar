/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.rabbitmq;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;

/**
 * RabbitMQ input adapter single port operator, which consume data from RabbitMQ message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have one output port<br>
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
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can emit about 10 thousand unique (k,v immutable pairs) tuples/sec as RabbitMQ DAG. The performance is directly proportional to key,val pairs emitted<br>
 * <br>
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractSinglePortRabbitMQInputOperator<T> extends AbstractRabbitMQInputOperator
{
    @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

  /**
   * Any concrete class derived from AbstractSinglePortRabbitMQInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   * It converts a byte message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param message
   */
    public abstract T getTuple(byte[] message);

    @Override
    public void emitTuple(byte[] message) {
      outputPort.emit(getTuple(message));
    }
}
