/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.ActivationListener;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.api.Operator;
import com.malhartech.api.Context.OperatorContext;
import java.util.ArrayList;

/**
 * Base class for input operator with a single output port without recovery.
 * <p>
 * Handles hand over from asynchronous input to port processing thread (tuples
 * must be emitted by container thread). If derived class implements
 * {@link Runnable} to perform synchronous IO, this class will manage the thread
 * according to the operator lifecycle.
 */
public class SimpleSinglePortInputOperator<T> extends BaseOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private transient Thread ioThread;
  private transient boolean isActive = false;

  /**
   * The single output port of this input operator.
   * Collects asynchronously emitted tuples and flushes in container thread.
   */
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient BufferingOutputPort<T> outputPort = new BufferingOutputPort<T>(this);

  @Override
  final public void activate(OperatorContext ctx)
  {
    isActive = true;
    if (this instanceof Runnable) {
      ioThread = new Thread((Runnable)this, "io-" + this.getName());
      ioThread.start();
    }
  }

  @Override
  final public void deactivate()
  {
    isActive = false;
    if (ioThread != null) {
      // thread to exit sleep or blocking IO
      ioThread.interrupt();
    }
  }

  final public boolean isActive()
  {
    return isActive;
  }

  @Override
  public void emitTuples()
  {
    outputPort.flush();
  }

  public static class BufferingOutputPort<T> extends DefaultOutputPort<T>
  {
    public ArrayList<T> tuples = new ArrayList<T>();

    /**
     * @param operator
     */
    public BufferingOutputPort(Operator operator)
    {
      super(operator);
    }

    @Override
    public synchronized void emit(T tuple)
    {
      tuples.add(tuple);
    }

    public synchronized void flush()
    {
      for (T tuple: tuples) {
        super.emit(tuple);
      }
      tuples.clear();
    }
  };
}