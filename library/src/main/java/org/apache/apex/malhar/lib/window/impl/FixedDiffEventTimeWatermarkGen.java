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

import javax.annotation.Nonnegative;

import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.ImplicitWatermarkGenerator;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;

/**
 * An @{@link ImplicitWatermarkGenerator} implementation for generating watermarks
 * based on event time.
 *
 * Generates a watermark tuple with a fixed difference from the latest event time.
 *
 * @since 3.7.0
 */
@InterfaceStability.Evolving
public class FixedDiffEventTimeWatermarkGen implements ImplicitWatermarkGenerator
{
  @Nonnegative
  private long fixedDifference;
  private long maxEventTime = -1;

  public FixedDiffEventTimeWatermarkGen(long fixedDifference)
  {
    this.fixedDifference = fixedDifference;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void processTupleForWatermark(Tuple.WindowedTuple t, long currentProcessingTime)
  {
    long eventTime = t.getTimestamp();
    if (maxEventTime < eventTime) {
      maxEventTime = eventTime;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ControlTuple.Watermark getWatermarkTuple(long currentProcessingTime)
  {
    return new WatermarkImpl(maxEventTime - fixedDifference);
  }

  @Override
  public void setup(Context context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public void setFixedDifference(long fixedDifference)
  {
    this.fixedDifference = fixedDifference;
  }
}
