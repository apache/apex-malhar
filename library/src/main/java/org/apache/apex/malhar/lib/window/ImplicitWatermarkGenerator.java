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
package org.apache.apex.malhar.lib.window;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Component;

/**
 * Interface for generators for implicit watermarks.
 * An operator which does not want to rely on explicit watermarks (generated from upstream),
 * can use implementations of this interface to get implicit watermarks.
 *
 * @since 3.7.0
 */
@InterfaceStability.Evolving
public interface ImplicitWatermarkGenerator extends Component
{
  /**
   * Called on arrival of every tuple.
   * Implementations would update the state of the watermark generator in order to keep their state updated
   * @param t the incoming windowed tuple
   * @param currentProcessingTime the current notion of processing time
   *                              (usually the system time generated based on the window id)
   */
  void processTupleForWatermark(Tuple.WindowedTuple t, long currentProcessingTime);

  /**
   * Returns the current watermark tuple as per the generator's state
   * @param currentProcessingTime the current notion of processing time
   *                              (usually the system time generated based on the window id)
   * @return the latest watermark tuple created based on the implementation
   */
  ControlTuple.Watermark getWatermarkTuple(long currentProcessingTime);
}
