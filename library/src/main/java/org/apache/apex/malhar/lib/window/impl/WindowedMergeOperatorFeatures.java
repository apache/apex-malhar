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

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.MergeAccumulation;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;

/**
 * This class provides the features in a MergeWindowedOperator and is intended to be used only
 * by the implementation of such operator
 */
abstract class WindowedMergeOperatorFeatures<InputT1, InputT2, AccumT, AccumulationT extends MergeAccumulation, DataStorageT extends WindowedStorage>
{
  protected AbstractWindowedOperator<InputT1, ?, DataStorageT, ?, AccumulationT> operator;

  protected long latestWatermark1 = -1;  // latest watermark from stream 1
  protected long latestWatermark2 = -1;  // latest watermark from stream 2

  protected abstract class AccumFunction<T>
  {
    abstract AccumT accumulate(AccumT accum, T value);
  }

  protected WindowedMergeOperatorFeatures()
  {
    // for kryo
  }

  WindowedMergeOperatorFeatures(AbstractWindowedOperator<InputT1, ?, DataStorageT, ?, AccumulationT> operator)
  {
    this.operator = operator;
  }

  abstract void accumulateTuple1(Tuple.WindowedTuple<InputT1> tuple);

  abstract void accumulateTuple2(Tuple.WindowedTuple<InputT2> tuple);

  void processWatermark1(ControlTuple.Watermark watermark)
  {
    latestWatermark1 = watermark.getTimestamp();
    // Select the smallest timestamp of the latest watermarks as the watermark of the operator.
    long minWatermark = Math.min(latestWatermark1, latestWatermark2);
    operator.setNextWatermark(minWatermark);
  }

  void processWatermark2(ControlTuple.Watermark watermark)
  {
    latestWatermark2 = watermark.getTimestamp();
    long minWatermark = Math.min(latestWatermark1, latestWatermark2);
    operator.setNextWatermark(minWatermark);
  }

  /**
   * The merge features for plain (non-keyed) operator
   */
  static class Plain<InputT1, InputT2, AccumT, AccumulationT extends MergeAccumulation<InputT1, InputT2, AccumT, ?>, DataStorageT extends WindowedStorage.WindowedPlainStorage<AccumT>>
      extends WindowedMergeOperatorFeatures<InputT1, InputT2, AccumT, AccumulationT, DataStorageT>
  {
    private Plain()
    {
      // for kryo
    }

    Plain(AbstractWindowedOperator<InputT1, ?, DataStorageT, ?, AccumulationT> operator)
    {
      super(operator);
    }

    private <T> void accumulateTupleHelper(Tuple.WindowedTuple<T> tuple, AccumFunction<T> accumFn)
    {
      for (Window window : tuple.getWindows()) {
        // process each window
        AccumT accum = operator.getDataStorage().get(window);
        if (accum == null) {
          accum = operator.getAccumulation().defaultAccumulatedValue();
        }
        operator.getDataStorage().put(window, accumFn.accumulate(accum, tuple.getValue()));
      }
    }

    @Override
    void accumulateTuple1(Tuple.WindowedTuple<InputT1> tuple)
    {
      accumulateTupleHelper(tuple, new AccumFunction<InputT1>()
      {
        @Override
        AccumT accumulate(AccumT accum, InputT1 value)
        {
          return operator.getAccumulation().accumulate(accum, value);
        }
      });
    }

    @Override
    void accumulateTuple2(Tuple.WindowedTuple<InputT2> tuple)
    {
      accumulateTupleHelper(tuple, new AccumFunction<InputT2>()
      {
        @Override
        AccumT accumulate(AccumT accum, InputT2 value)
        {
          return operator.getAccumulation().accumulate2(accum, value);
        }
      });
    }
  }

  /**
   * The merge features for keyed operator
   */
  static class Keyed<KeyT, InputT1, InputT2, AccumT, AccumulationT extends MergeAccumulation<InputT1, InputT2, AccumT, ?>, DataStorageT extends WindowedStorage.WindowedKeyedStorage<KeyT, AccumT>>
      extends WindowedMergeOperatorFeatures<KeyValPair<KeyT, InputT1>, KeyValPair<KeyT, InputT2>, AccumT, AccumulationT, DataStorageT>
  {
    private Keyed()
    {
      // for kryo
    }

    Keyed(AbstractWindowedOperator<KeyValPair<KeyT, InputT1>, ?, DataStorageT, ?, AccumulationT> operator)
    {
      super(operator);
    }

    private <T> void accumulateTupleHelper(Tuple.WindowedTuple<KeyValPair<KeyT, T>> tuple, AccumFunction<T> accumFn)
    {
      final KeyValPair<KeyT, T> kvData = tuple.getValue();
      KeyT key = kvData.getKey();
      for (Window window : tuple.getWindows()) {
        // process each window
        AccumT accum = operator.getDataStorage().get(window, key);
        if (accum == null) {
          accum = operator.getAccumulation().defaultAccumulatedValue();
        }
        operator.getDataStorage().put(window, key, accumFn.accumulate(accum, kvData.getValue()));
      }
    }

    @Override
    void accumulateTuple1(Tuple.WindowedTuple<KeyValPair<KeyT, InputT1>> tuple)
    {
      accumulateTupleHelper(tuple, new AccumFunction<InputT1>()
      {
        @Override
        AccumT accumulate(AccumT accum, InputT1 value)
        {
          return operator.getAccumulation().accumulate(accum, value);
        }
      });
    }

    @Override
    void accumulateTuple2(Tuple.WindowedTuple<KeyValPair<KeyT, InputT2>> tuple)
    {
      accumulateTupleHelper(tuple, new AccumFunction<InputT2>()
      {
        @Override
        AccumT accumulate(AccumT accum, InputT2 value)
        {
          return operator.getAccumulation().accumulate2(accum, value);
        }
      });
    }
  }
}
