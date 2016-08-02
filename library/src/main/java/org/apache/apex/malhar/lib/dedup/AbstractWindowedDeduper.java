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
package org.apache.apex.malhar.lib.dedup;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Abstract implementation for the Windowed Dedup operator
 *
 * @param <T>
 */
public abstract class AbstractWindowedDeduper<T> extends WindowedOperatorImpl<T, List<T>, T>
{

  /**
   * The output port on which unique events are emitted.
   */
  public final transient DefaultOutputPort<T> unique = new DefaultOutputPort<T>();

  /**
   * The output port on which duplicate events are emitted.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<T> duplicates = new DefaultOutputPort<T>();

  /**
   * The output port on which expired events are emitted.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<T> expired = new DefaultOutputPort<T>();

  /**
   * Returns the key of the tuple based on which the tuples are de-duplicated
   */
  protected abstract Object getKey(T tuple);

  /**
   * Insead of dropping, emit on expired port
   */
  @Override
  public void dropTuple(Tuple<T> input)
  {
    expired.emit(input.getValue());
  }

  /**
   * Performs the de-duplication
   */
  @Override
  public void accumulateTuple(Tuple.WindowedTuple<T> tuple)
  {
    for (Window window : tuple.getWindows()) { // There will be exactly one window
      // process each window
      List<T> accum = dataStorage.get(window);
      if (accum == null) {
        // New window?
        accum = new ArrayList<T>();
        unique.emit(tuple.getValue());
        dataStorage.put(window, accumulation.accumulate(accum, tuple.getValue()));
      } else {
        Object tupleKey = getKey(tuple.getValue());
        for (T key: accum) {
          if (getKey(key).equals(tupleKey)) {
            duplicates.emit(tuple.getValue());
            return;
          }
        }
        unique.emit(tuple.getValue());
        dataStorage.put(window, accumulation.accumulate(accum, tuple.getValue()));
      }
    }
  }
}
