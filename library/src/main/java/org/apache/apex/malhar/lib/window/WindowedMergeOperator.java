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

/**
 * Interface for Merge Windowed Operator.
 *
 * @since 3.6.0
 */
public interface WindowedMergeOperator<InputT1, InputT2>
    extends WindowedOperator<InputT1>
{
  /**
   * The method to accumulate the data tuple from the 2nd input stream
   *
   * @param tuple the data tuple
   */
  void accumulateTuple2(Tuple.WindowedTuple<InputT2> tuple);

  /**
   * The method to process the watermark tuple from the 2nd input stream
   *
   * @param watermark the watermark tuple
   */
  void processWatermark2(ControlTuple.Watermark watermark);
}
