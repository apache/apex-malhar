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
package org.apache.apex.malhar.lib.multiwindow;

import java.util.ArrayList;

import javax.validation.constraints.Min;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 *
 * <p>Provides a sliding window class that lets users access both states of all streaming window in current sliding window
 * and state of the expired streaming window from last sliding windows. </p>
 * <p>
 * <b>Properties</b>:<br>
 * <b>T</b> is the tuple object the operator accept <br>
 * <b>S</b> is the state object kept in the sliding window <br>
 * <b>windowSize</b>: Number of streaming window in this sliding window<br>
 * <br>
 *
 * @displayName Abstract Sliding Window
 * @category Stats and Aggregations
 * @tags sliding window, state
 * @since 0.3.3
 */
public abstract class AbstractSlidingWindow<T, S> extends BaseOperator
{
        /**
         * Input port for getting incoming data.
         */
  public final transient DefaultInputPort<T> data = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processDataTuple(tuple);
    }
  };

  protected ArrayList<S> states = null;

  protected S lastExpiredWindowState = null;

  protected int currentCursor = -1;

  @Min(2)
  int windowSize = 2;

  /**
   * getter function for n (number of previous window states
   *
   * @return n
   */
  @Min(2)
  public int getWindowSize()
  {
    return windowSize;
  }

  /**
   * setter for windowSize
   *
   * @param windowSize
   */
  public void setWindowSize(int windowSize)
  {
    this.windowSize = windowSize;
  }

  protected abstract void processDataTuple(T tuple);

  /**
   * Implement this method to create the state object needs to be kept in the sliding window
   *
   * @return the state of current streaming window
   */
  public abstract S createWindowState();

  /**
   * Get the Streaming window state in it's coming the order start from 0
   *
   * @param i
   *   0 the state of the first coming streaming window
   *   -1 the state of the last expired streaming window
   * @return State of the streaming window
   * @throws ArrayIndexOutOfBoundsException if i >= sliding window size
   */
  public S getStreamingWindowState(int i)
  {
    if (i == -1) {
      return lastExpiredWindowState;
    }
    if (i >= getWindowSize()) {
      throw new ArrayIndexOutOfBoundsException();
    }
    int index = (currentCursor + 1 + i) % windowSize;
    return states.get(index);
  }

  /**
   * Moves states by 1 and sets current state to null. If you override
   * beginWindow, you must call super.beginWindow(windowId) to ensure proper
   * operator behavior.
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    // move currentCursor 1 position
    currentCursor = (currentCursor + 1) % windowSize;
    // expire the state at the first position which is the state of the streaming window moving out of the current application window
    lastExpiredWindowState = states.get(currentCursor);

    states.set(currentCursor, createWindowState());

  }

  /**
   * Sets up internal state structure
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    states = new ArrayList<S>(windowSize);
    //initialize the sliding window state to null
    for (int i = 0; i < windowSize; i++) {
      states.add(null);
    }
    currentCursor = -1;
  }
}
