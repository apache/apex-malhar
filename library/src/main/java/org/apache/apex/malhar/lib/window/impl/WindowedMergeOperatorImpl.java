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

import org.apache.apex.malhar.lib.window.MergeAccumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;

/**
 * Windowed Merge Operator to merge two streams together. It aggregates tuple from two
 * input streams, perform merge operation base on its merge accumulation, and output one
 * result stream to downstream.
 *
 * @param <InputT1> The type of input tuple from first stream.
 * @param <InputT2> The type of input tuple from first stream.
 * @param <AccumT> The type of the accumulated value in the operator state per key per window.
 * @param <OutputT> The type of output tuple.
 */
public class WindowedMergeOperatorImpl<InputT1, InputT2, AccumT, OutputT>
    extends AbstractWindowedMergeOperator<InputT1, InputT2, OutputT, WindowedStorage.WindowedPlainStorage<AccumT>, WindowedStorage.WindowedPlainStorage<OutputT>, MergeAccumulation<InputT1, InputT2, AccumT, OutputT>>
{
  private abstract class AccumFunction<T>
  {
    abstract AccumT accumulate(AccumT accum, T value);
  }

  private <T> void accumulateTupleHelper(Tuple.WindowedTuple<T> tuple, AccumFunction<T> accumFn)
  {
    for (Window window : tuple.getWindows()) {
      // process each window
      AccumT accum = dataStorage.get(window);
      if (accum == null) {
        accum = accumulation.defaultAccumulatedValue();
      }
      dataStorage.put(window, accumFn.accumulate(accum, tuple.getValue()));
    }
  }

  @Override
  public void accumulateTuple2(Tuple.WindowedTuple<InputT2> tuple)
  {
    accumulateTupleHelper(tuple, new AccumFunction<InputT2>()
    {
      @Override
      AccumT accumulate(AccumT accum, InputT2 value)
      {
        return accumulation.accumulate2(accum, value);
      }
    });
  }

  @Override
  public void accumulateTuple(Tuple.WindowedTuple<InputT1> tuple)
  {
    accumulateTupleHelper(tuple, new AccumFunction<InputT1>()
    {
      @Override
      AccumT accumulate(AccumT accum, InputT1 value)
      {
        return accumulation.accumulate(accum, value);
      }
    });
  }

  @Override
  public void fireNormalTrigger(Window window, boolean fireOnlyUpdatedPanes)
  {
    AccumT accumulatedValue = dataStorage.get(window);
    OutputT outputValue = accumulation.getOutput(accumulatedValue);

    if (fireOnlyUpdatedPanes) {
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
  public void fireRetractionTrigger(Window window, boolean firingOnlyUpdatedPanes)
  {
    if (triggerOption.getAccumulationMode() != TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      throw new UnsupportedOperationException();
    }
    OutputT oldValue = retractionStorage.get(window);
    if (oldValue != null) {
      output.emit(new Tuple.WindowedTuple<>(window, accumulation.getRetraction(oldValue)));
    }
  }
}
