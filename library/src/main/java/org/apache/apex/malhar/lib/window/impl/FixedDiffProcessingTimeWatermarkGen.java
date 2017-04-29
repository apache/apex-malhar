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

import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.ImplicitWatermarkGenerator;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;

/**
 * An @{@link ImplicitWatermarkGenerator} implementation for generating watermarks
 * based on processing time.
 *
 * Generates a watermark tuple with a fixed difference from the current processing time.
 *
 * @since 3.7.0
 */
@InterfaceStability.Evolving
public class FixedDiffProcessingTimeWatermarkGen implements ImplicitWatermarkGenerator
{
  long fixedWatermarkMillis = -1;

  public FixedDiffProcessingTimeWatermarkGen(long fixedWatermarkMillis)
  {
    this.fixedWatermarkMillis = fixedWatermarkMillis;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void processTupleForWatermark(Tuple.WindowedTuple t, long currentProcessingTime)
  {
    // do nothing
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ControlTuple.Watermark getWatermarkTuple(long currentProcessingTime)
  {
    return new WatermarkImpl(currentProcessingTime - fixedWatermarkMillis);
  }

  @Override
  public void setup(Context context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public void setFixedWatermarkMillis(long fixedWatermarkMillis)
  {
    this.fixedWatermarkMillis = fixedWatermarkMillis;
  }
}
