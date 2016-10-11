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

import java.util.Map;

import org.apache.apex.malhar.lib.window.MergeAccumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;

import com.datatorrent.lib.util.KeyValPair;


/**
 * Keyed Windowed Merge Operator to merge two streams of keyed tuple with a key. Please use
 * {@link WindowedMergeOperatorImpl} for non-keyed merging.
 *
 * @param <KeyT> Type of the key used to merge two streams.
 * @param <InputT1> The type of the value of the keyed input tuple from first stream.
 * @param <InputT2> The type of the value of the keyed input tuple from second stream.
 * @param <AccumT> The type of the accumulated value in the operator state per key per window.
 * @param <OutputT> The type of the value of the keyed output tuple.
 */
public class KeyedWindowedMergeOperatorImpl<KeyT, InputT1, InputT2, AccumT, OutputT>
    extends AbstractWindowedMergeOperator<KeyValPair<KeyT, InputT1>, KeyValPair<KeyT, InputT2>, KeyValPair<KeyT, OutputT>, WindowedStorage.WindowedKeyedStorage<KeyT, AccumT>, WindowedStorage.WindowedKeyedStorage<KeyT, OutputT>, MergeAccumulation<InputT1, InputT2, AccumT, OutputT>>
{
  // TODO: Add session window support.

  private abstract class AccumFunction<T>
  {
    abstract AccumT accumulate(AccumT accum, T value);
  }

  private <T> void accumulateTupleHelper(Tuple.WindowedTuple<KeyValPair<KeyT, T>> tuple, AccumFunction<T> accumFn)
  {
    final KeyValPair<KeyT, T> kvData = tuple.getValue();
    KeyT key = kvData.getKey();
    for (Window window : tuple.getWindows()) {
      // process each window
      AccumT accum = dataStorage.get(window, key);
      if (accum == null) {
        accum = accumulation.defaultAccumulatedValue();
      }
      dataStorage.put(window, key, accumFn.accumulate(accum, kvData.getValue()));
    }
  }

  @Override
  public void accumulateTuple(Tuple.WindowedTuple<KeyValPair<KeyT, InputT1>> tuple)
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
  public void accumulateTuple2(Tuple.WindowedTuple<KeyValPair<KeyT, InputT2>> tuple)
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
  public void fireNormalTrigger(Window window, boolean fireOnlyUpdatedPanes)
  {
    for (Map.Entry<KeyT, AccumT> entry : dataStorage.entries(window)) {
      OutputT outputVal = accumulation.getOutput(entry.getValue());
      if (fireOnlyUpdatedPanes) {
        OutputT oldValue = retractionStorage.get(window, entry.getKey());
        if (oldValue != null && oldValue.equals(outputVal)) {
          continue;
        }
      }
      output.emit(new Tuple.WindowedTuple<>(window, new KeyValPair<>(entry.getKey(), outputVal)));
      if (retractionStorage != null) {
        retractionStorage.put(window, entry.getKey(), outputVal);
      }
    }
  }

  @Override
  public void fireRetractionTrigger(Window window, boolean firingOnlyUpdatedPanes)
  {
    if (triggerOption.getAccumulationMode() != TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      throw new UnsupportedOperationException();
    }
    for (Map.Entry<KeyT, OutputT> entry : retractionStorage.entries(window)) {
      output.emit(new Tuple.WindowedTuple<>(window, new KeyValPair<>(entry.getKey(), accumulation.getRetraction(entry.getValue()))));
    }
  }
}
