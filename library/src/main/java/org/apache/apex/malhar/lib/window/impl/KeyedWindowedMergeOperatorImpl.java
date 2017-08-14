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
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowedMergeOperator;

import com.google.common.base.Function;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;


/**
 * Keyed Windowed Merge Operator to merge two streams of keyed tuple with a key. Please use
 * {@link WindowedMergeOperatorImpl} for non-keyed merging.
 *
 * @param <KeyT> Type of the key used to merge two streams.
 * @param <InputT1> The type of the value of the keyed input tuple from first stream.
 * @param <InputT2> The type of the value of the keyed input tuple from second stream.
 * @param <AccumT> The type of the accumulated value in the operator state per key per window.
 * @param <OutputT> The type of the value of the keyed output tuple.
 *
 * @since 3.6.0
 */
public class KeyedWindowedMergeOperatorImpl<KeyT, InputT1, InputT2, AccumT, OutputT>
    extends KeyedWindowedOperatorImpl<KeyT, InputT1, AccumT, OutputT>
    implements WindowedMergeOperator<KeyValPair<KeyT, InputT1>, KeyValPair<KeyT, InputT2>>
{
  private Function<KeyValPair<KeyT, InputT2>, Long> timestampExtractor2;

  private WindowedMergeOperatorFeatures.Keyed joinFeatures = new WindowedMergeOperatorFeatures.Keyed(this);

  public final transient DefaultInputPort<Tuple<KeyValPair<KeyT, InputT2>>> input2 = new DefaultInputPort<Tuple<KeyValPair<KeyT, InputT2>>>()
  {
    @Override
    public void process(Tuple<KeyValPair<KeyT, InputT2>> tuple)
    {
      processTuple2(tuple);
    }
  };

  // TODO: This port should be removed when Apex Core has native support for custom control tuples
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<ControlTuple> controlInput2 = new DefaultInputPort<ControlTuple>()
  {
    @Override
    public void process(ControlTuple tuple)
    {
      if (tuple instanceof ControlTuple.Watermark) {
        processWatermark2((ControlTuple.Watermark)tuple);
      }
    }
  };

  public void setTimestampExtractor2(Function<KeyValPair<KeyT, InputT2>, Long> timestampExtractor)
  {
    this.timestampExtractor2 = timestampExtractor;
  }

  public void processTuple2(Tuple<KeyValPair<KeyT, InputT2>> tuple)
  {
    long timestamp = extractTimestamp(tuple, this.timestampExtractor2);
    if (isTooLate(timestamp)) {
      dropTuple(tuple);
    } else {
      Tuple.WindowedTuple<KeyValPair<KeyT, InputT2>> windowedTuple = getWindowedValueWithTimestamp(tuple, timestamp);
      // do the accumulation
      accumulateTuple2(windowedTuple);
      processWindowState(windowedTuple);
    }
  }

  @Override
  public void accumulateTuple2(Tuple.WindowedTuple<KeyValPair<KeyT, InputT2>> tuple)
  {
    joinFeatures.accumulateTuple2(tuple);
  }

  @Override
  public void processWatermark(ControlTuple.Watermark watermark)
  {
    joinFeatures.processWatermark1(watermark);
  }

  @Override
  public void processWatermark2(ControlTuple.Watermark watermark)
  {
    joinFeatures.processWatermark2(watermark);
  }
}
