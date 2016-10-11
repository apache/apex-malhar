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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.MergeAccumulation;
import org.apache.apex.malhar.lib.window.MergeWindowedOperator;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowedStorage;

import com.google.common.base.Function;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;


/**
 * Abstract Windowed Merge Operator.
 */
public abstract class AbstractWindowedMergeOperator<InputT1, InputT2, OutputT, DataStorageT extends WindowedStorage,
    RetractionStorageT extends WindowedStorage, AccumulationT extends
    MergeAccumulation>
    extends AbstractWindowedOperator<InputT1, OutputT, DataStorageT, RetractionStorageT, AccumulationT>
    implements MergeWindowedOperator<InputT1, InputT2>
{

  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractWindowedMergeOperator.class);
  private Function<InputT2, Long> timestampExtractor2;

  private long latestWatermark1 = -1;  // latest watermark from stream 1
  private long latestWatermark2 = -1;  // latest watermark from stream 2

  public final transient DefaultInputPort<Tuple<InputT2>> input2 = new DefaultInputPort<Tuple<InputT2>>()
  {
    @Override
    public void process(Tuple<InputT2> tuple)
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

  public void processTuple2(Tuple<InputT2> tuple)
  {
    long timestamp = extractTimestamp(tuple, timestampExtractor2);
    if (isTooLate(timestamp)) {
      dropTuple(tuple);
    } else {
      Tuple.WindowedTuple<InputT2> windowedTuple = getWindowedValueWithTimestamp(tuple, timestamp);
      // do the accumulation
      accumulateTuple2(windowedTuple);
      processWindowState(windowedTuple);
    }
  }

  public void setTimestampExtractor2(Function<InputT2, Long> timestampExtractor2)
  {
    this.timestampExtractor2 = timestampExtractor2;
  }


  @Override
  public void processWatermark(ControlTuple.Watermark watermark)
  {
    latestWatermark1 = watermark.getTimestamp();
    if (latestWatermark1 >= 0 && latestWatermark2 >= 0) {
      // Select the smallest timestamp of the latest watermarks as the watermark of the operator.
      long minWatermark = Math.min(latestWatermark1, latestWatermark2);
      if (this.watermarkTimestamp < 0 || minWatermark != this.watermarkTimestamp) {
        this.watermarkTimestamp = minWatermark;
      }
    }
  }

  @Override
  public void processWatermark2(ControlTuple.Watermark watermark)
  {
    latestWatermark2 = watermark.getTimestamp();
    if (latestWatermark1 >= 0 && latestWatermark2 >= 0) {
      long minWatermark = Math.min(latestWatermark1, latestWatermark2);
      if (this.watermarkTimestamp < 0 || minWatermark != this.watermarkTimestamp) {
        this.watermarkTimestamp = minWatermark;
      }
    }
  }

  @Override
  protected void processWatermarkAtEndWindow()
  {
    if (fixedWatermarkMillis > 0 || this.watermarkTimestamp != this.currentWatermark) {
      super.processWatermarkAtEndWindow();
    }
  }
}
