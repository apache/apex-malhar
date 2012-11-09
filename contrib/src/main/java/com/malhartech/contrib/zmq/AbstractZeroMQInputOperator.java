/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractZeroMQInputOperator<T> extends AbstractBaseZeroMQInputOperator
{
    @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);
  private int tuple_blast = 1000;

  public abstract void emitMessage(byte[] message);

  public void emitTuple(T tuple)
  {
    outputPort.emit(getOutputTuple(tempBuffer.pollUnsafe()));
   }

  // This is the beginWindow, <emitTuples: one or many as per time>, endWindow thread
  @Override
  public void emitTuples()
  {
    int ntuples = tuple_blast;
    if (ntuples > tempBuffer.size()) {
      ntuples = tempBuffer.size();
    }
    for (int i = ntuples; i-- > 0;) {
      outputPort.emit(getOutputTuple(tempBuffer.pollUnsafe()));
    }
  }
}
