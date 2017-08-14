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
package org.apache.apex.malhar.contrib.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;

/**
 * @param <T> the type of tuple
 */
public class POJOTupleGenerateOperator<T> implements InputOperator, ActivationListener<OperatorContext>
{
  protected final int DEFAULT_TUPLE_NUM = 10000;
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  private int tupleNum = DEFAULT_TUPLE_NUM;
  private int batchNum = 5;
  private TupleGenerator<T> tupleGenerator = null;
  private Class<T> tupleClass;
  private AtomicInteger emitedTuples = new AtomicInteger(0);

  public POJOTupleGenerateOperator()
  {
  }

  public POJOTupleGenerateOperator( Class<T> tupleClass )
  {
    this.tupleClass = tupleClass;
  }

  public void setTupleType( Class<T> tupleClass )
  {
    this.tupleClass = tupleClass;
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(OperatorContext ctx)
  {
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void emitTuples()
  {
    final int theTupleNum = getTupleNum();
    if ( emitedTuples.get() >= theTupleNum ) {
      try {
        Thread.sleep(10);
      } catch ( Exception e ) {
        //
      }
      return;
    }


    for ( int i = 0; i < batchNum; ++i ) {
      int count = emitedTuples.get();
      if ( count >= theTupleNum ) {
        return;
      }
      if ( emitedTuples.compareAndSet(count, count + 1) ) {
        T tuple = getNextTuple();
        outputPort.emit( tuple );
        tupleEmitted( tuple );

        if ( count + 1 == theTupleNum ) {
          tupleEmitDone();
          return;
        }
      }

    }
  }


  protected void tupleEmitted( T tuple ){}

  protected void tupleEmitDone() {}

  public int getEmitedTupleCount()
  {
    return emitedTuples.get();
  }

  public int getTupleNum()
  {
    return tupleNum;
  }

  public void setTupleNum( int tupleNum )
  {
    this.tupleNum = tupleNum;
  }

  protected T getNextTuple()
  {
    if ( tupleGenerator == null ) {
      tupleGenerator = createTupleGenerator();
    }
    return tupleGenerator.getNextTuple();
  }

  protected TupleGenerator<T> createTupleGenerator()
  {
    return new TupleGenerator( tupleClass );
  }
}
