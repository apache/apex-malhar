/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.io;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.lang3.ClassUtils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This an input operator which passes data from an asynchronous data source to a port processing thread.
 * <p>
 * This operator handles hand over from asynchronous input to port processing thread (tuples
 * must be emitted by container thread). If derived class implements
 * {@link Runnable} to perform synchronous IO, this class will manage the thread
 * according to the operator lifecycle.
 * </p>
 * @displayName Asynchronous Input Processing
 * @category Input
 *
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class SimpleSinglePortInputOperator<T> extends BaseOperator implements InputOperator, Operator.ActivationListener<OperatorContext>
{
  private transient Thread ioThread;
  private transient boolean isActive = false;
  /**
   * The single output port of this input operator.
   * Collects asynchronously emitted tuples and flushes in container thread.
   */
  public final transient BufferingOutputPort<T> outputPort;

  public SimpleSinglePortInputOperator(int portCapacity)
  {
    outputPort = new BufferingOutputPort<T>(this, portCapacity);
  }

  public SimpleSinglePortInputOperator()
  {
    this(1024);
  }

  @Override
  public final void activate(OperatorContext ctx)
  {
    isActive = true;
    if (this instanceof Runnable) {
      ioThread = new Thread((Runnable)this, "io-" + ClassUtils.getShortClassName(this.getClass()));
      ioThread.start();
    }
  }

  @Override
  public final void deactivate()
  {
    isActive = false;
    if (ioThread != null) {
      // thread to exit sleep or blocking IO
      ioThread.interrupt();
    }
  }

  public final boolean isActive()
  {
    return isActive;
  }

  @Override
  public void emitTuples()
  {
    outputPort.flush(Integer.MAX_VALUE);
  }

  public static class BufferingOutputPort<T> extends DefaultOutputPort<T>
  {
    public final ArrayBlockingQueue<T> tuples;

    /**
     * @param operator
     */
    public BufferingOutputPort(Operator operator)
    {
      super();
      tuples = new ArrayBlockingQueue<T>(1024);
    }

    public BufferingOutputPort(Operator operator, int capacity)
    {
      super();
      tuples = new ArrayBlockingQueue<T>(capacity);
    }

    @Override
    public void emit(T tuple)
    {
      try {
        tuples.put(tuple);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

    public void flush(int count)
    {
      Iterator<T> iterator = tuples.iterator();
      while (count-- > 0 && iterator.hasNext()) {
        super.emit(iterator.next());
        iterator.remove();
      }
    }

  }

}
