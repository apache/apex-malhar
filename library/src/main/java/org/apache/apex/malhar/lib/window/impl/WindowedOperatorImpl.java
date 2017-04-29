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
package org.apache.apex.malhar.lib.window.impl;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is an implementation of the WindowedOperator. If your operation is key based, please use {@link KeyedWindowedOperatorImpl}.
 *
 * @param <InputT> The type of the value of the input tuple
 * @param <AccumT> The type of the accumulated value in the operator state per window
 * @param <OutputT> The type of the value of the output tuple
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class WindowedOperatorImpl<InputT, AccumT, OutputT>
    extends AbstractWindowedOperator<InputT, OutputT, WindowedStorage.WindowedPlainStorage<AccumT>, WindowedStorage.WindowedPlainStorage<OutputT>, Accumulation<? super InputT, AccumT, OutputT>>
{
  @Override
  public void accumulateTuple(Tuple.WindowedTuple<InputT> tuple)
  {
    for (Window window : tuple.getWindows()) {
      // process each window
      AccumT accum = dataStorage.get(window);
      if (accum == null) {
        accum = accumulation.defaultAccumulatedValue();
      }
      dataStorage.put(window, accumulation.accumulate(accum, tuple.getValue()));
    }
  }

  @Override
  public void fireNormalTrigger(Window window, boolean fireOnlyUpdatedPanes)
  {
    AccumT accumulatedValue = dataStorage.get(window);
    OutputT outputValue = accumulation.getOutput(accumulatedValue);
    if (fireOnlyUpdatedPanes && retractionStorage != null) {
      OutputT oldValue = retractionStorage.get(window);
      if (oldValue != null && oldValue.equals(outputValue)) {
        return;
      }
    }
    output.emit(new Tuple.WindowedTuple<>(window, outputValue));
    if (retractionStorage != null) {
      retractionStorage.put(window, outputValue);
    }
  }

  @Override
  public void fireRetractionTrigger(Window window, boolean fireOnlyUpdatedPanes)
  {
    if (triggerOption.getAccumulationMode() != TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      throw new UnsupportedOperationException();
    }
    OutputT oldValue = retractionStorage.get(window);
    if (oldValue != null) {
      if (fireOnlyUpdatedPanes) {
        AccumT accumulatedValue = dataStorage.get(window);
        if (accumulatedValue != null && oldValue.equals(accumulation.getOutput(accumulatedValue))) {
          return;
        }
      }
      output.emit(new Tuple.WindowedTuple<>(window, accumulation.getRetraction(oldValue)));
    }
  }
}
