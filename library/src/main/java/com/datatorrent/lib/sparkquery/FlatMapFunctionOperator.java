/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.sparkquery;

import java.util.Iterator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.sparkquery.mapfunction.MapFunctionCall;


/**
 * <p>
 * Class to implement Spark RDD(Resilient Distributed Dataset) faltMap member function semantic in library.  <br>
 * In DataTorrent framework, tuples accumulated over application window represent RRD in spark stream platform. <br>
 * <br>
 * 
 */
public class FlatMapFunctionOperator<T, R> implements Operator
{
  /**
   * Map function  call. 
   */
  private MapFunctionCall<T, R> call;
  
  /**
   * Input port.
   */
  public final transient DefaultInputPort<T> inport = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      Iterable<R> iterable = call.call(tuple);
      Iterator<R>  iter = iterable.iterator();
      while (iter.hasNext()) {
        outport.emit(iter.next());
      }
    }
  };
  
  /**
   * Output port.
   */
  public final transient DefaultOutputPort<R> outport = new DefaultOutputPort<R>();
  
  /**
   * @see com.datatorrent.api.Component#setup(com.datatorrent.api.Context)
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * @see com.datatorrent.api.Component#teardown()
   */
  @Override
  public void teardown()
  {
  }

  /**
   * @see com.datatorrent.api.Operator#beginWindow(long)
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * @see com.datatorrent.api.Operator#endWindow()
   */
  @Override
  public void endWindow()
  {
  }

  /**
   * Get value for call.
   * @return MapFunctionCall
   */
  public MapFunctionCall<T, R> getCall()
  {
    return call;
  }

  /**
   * Set value for call.
   * @param call set value for call.
   */
  public void setCall(MapFunctionCall<T, R> call)
  {
    this.call = call;
  }

}
