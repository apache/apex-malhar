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
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.util.BaseNumberKeyValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.DefaultInputPort;

/**
 *
 * Provides a sliding window class that lets users access past N-1 window states where N is a property.
 * <p>
 * The default behavior is just a pass through, i.e. the
 * operator does not do any processing on its own. Users are expected to extend
 * this class and add their specific processing. Users have to define their own
 * output port(s). The tuples are KeyValue pair. This is an abstract class. The
 * concrete class has to provide the interface SlidingWindowObject, which keeps
 * information about each window.<br>
 * This module is end of window as per users choice<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects T (any POJO)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>windowSize i.e. N</b>: Number of windows to keep state on<br>
 * <br>
 * @displayName Abstract Sliding Window Key Value
 * @category Stats and Aggregations
 * @tags sliding window, numeric, key value, average
 * @since 0.3.3
 */
public abstract class AbstractSlidingWindowKeyVal<K, V extends Number, S extends SimpleMovingAverageObject>
    extends BaseNumberKeyValueOperator<K, V>
{
  /**
   * buffer to hold state information of different windows.
   */
  protected HashMap<K, ArrayList<S>> buffer = new HashMap<K, ArrayList<S>>();
  /**
   * Index of windows stating at 0.
   */
  protected int currentstate = -1;

  /**
   * Concrete class has to implement how they want the tuple to be processed.
   *
   * @param tuple
   *          a keyVal pair of tuple.
   */
  public abstract void processDataTuple(KeyValPair<K, V> tuple);

  /**
   * Concrete class has to implement what to emit at the end of window.
   *
   * @param key
   * @param obj
   */
  public abstract void emitTuple(K key, ArrayList<S> obj);

  /**
   * Length of sliding windows. Minimum value is 2.
   */
  @Min(2)
  protected int windowSize = 2;
  protected long windowId;

  /**
   * Getter function for windowSize (number of previous window buffer).
   *
   * @return windowSize
   */
  public int getWindowSize()
  {
    return windowSize;
  }

  /**
   * @param windowSize
   */
  public void setWindowSize(int windowSize)
  {
    this.windowSize = windowSize;
  }

  /**
   * Input port for getting incoming data.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>()
  {
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      processDataTuple(tuple);
    }
  };

  /**
   * Moves buffer by 1 and clear contents of current. If you override
   * beginWindow, you must call super.beginWindow(windowId) to ensure proper
   * operator behavior.
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    currentstate++;
    if (currentstate >= windowSize) {
      for (Map.Entry<K, ArrayList<S>> e : buffer.entrySet()) {
        ArrayList<S> states = e.getValue();
        S first = states.get(0);
        for (int i = 1; i < windowSize; i++) {
          states.set(i - 1, states.get(i));
        }
        states.set(windowSize - 1, first);
      }
      currentstate = windowSize - 1;
    }
    for (Map.Entry<K, ArrayList<S>> e : buffer.entrySet()) {
      e.getValue().get(currentstate).clear();
    }
  }

  /**
   * Emit tuple for each key.
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, ArrayList<S>> e : buffer.entrySet()) {
      emitTuple(e.getKey(), e.getValue());
    }
  }
}
