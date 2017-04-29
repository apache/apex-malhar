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

/**
 * This is the interface for accumulation when joining two streams.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface MergeAccumulation<InputT1, InputT2, AccumT, OutputT> extends Accumulation<InputT1, AccumT, OutputT>
{
  /**
   * Accumulate the second input type to the accumulated value
   *
   * @param accumulatedValue
   * @param input
   * @return
   */
  AccumT accumulate2(AccumT accumulatedValue, InputT2 input);

}
