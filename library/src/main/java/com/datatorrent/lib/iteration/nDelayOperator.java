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

package com.datatorrent.lib.iteration;

import java.io.IOException;
import java.util.ArrayList;
import javax.validation.constraints.Min;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.util.WindowDataManager;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This operator is used in iteration and can do n window delay.
 * Required delay needs to be set in the constructor, default delay is 1.
  *
 * @displayName nDelayOperator
 * @category iteration

 */
public class nDelayOperator<T> implements Operator.DelayOperator, Operator.CheckpointListener
{
  private WindowDataManager windowDataManager = new WindowDataManager.FSWindowDataManager();
  @Min(1)
  private int delay = 1;
  private long currentWindowId;
  private transient int operatorContextId;
  private transient ArrayList<T> windowData;
  private transient Context.OperatorContext context;

  public transient DefaultInputPort<T> input = new DefaultInputPort<T>() {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }
  };

  public transient DefaultOutputPort<T> output = new DefaultOutputPort();

  public nDelayOperator()
  {
    init();
  }

  /*
  * @param delay set the delay in number of windows for the tuples.
   */
  public nDelayOperator(int delay)
  {
    if ( delay < 1 ) {
      throw new IllegalArgumentException("Invalid Delay specified.");
    }
    this.delay = delay;
    init();
  }

  /*
* Get Window Data Manager instance
*/
  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  /*
  * Set Window Data Manager instance
  */
  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  private void init()
  {
    windowData = new ArrayList<>();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.operatorContextId = context.getId();
    this.windowDataManager.setup(context);
    this.context = context;
  }

  @Override
  public void teardown()
  {
    this.windowDataManager.teardown();
  }

  @Override
  public void firstWindow()
  {
    replay(currentWindowId - delay);
  }

  private void replay( long windowId )
  {
    if ( windowId < 0 ) {
      return;
    }

    ArrayList<T> recoveredData;
    try {
      recoveredData = (ArrayList<T>)this.windowDataManager.load(operatorContextId, windowId);
      if (recoveredData == null) {
        return;
      }
      for ( T tuple : recoveredData) {
        output.emit(tuple);
      }
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;

    if ( delay > 1 ) {
      replay(windowId - delay + 1);
    }
  }

  @Override
  public void endWindow()
  {
    try {
      this.windowDataManager.save(windowData, operatorContextId, currentWindowId);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }

    windowData.clear();
  }

  protected void processTuple(T tuple)
  {
    windowData.add(tuple);

    if ( delay == 1 ) {
      output.emit(tuple);
    }
  }

  @Override
  public void checkpointed(long l)
  {

  }

  @Override
  public void committed(long windowId)
  {
    try {
      windowDataManager.deleteUpTo(operatorContextId, windowId - delay - 1);
    } catch (IOException e) {
      throw new RuntimeException("committing", e);
    }
  }
}

