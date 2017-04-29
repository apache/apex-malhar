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
 * This interface is for the processing part of the WindowedOperator.
 * We can assume that all stateful processing of the WindowedOperator is a form of accumulation.
 *
 * In most cases, AccumT is the same as OutputT. But in some cases, the accumulated type and the output type may be
 * different. For example, if we are doing the AVERAGE of doubles, InputT will be double, and we need the SUM and the
 * COUNT stored as type AccumT, and AccumT will be a pair of double and long, in which double is the sum of the inputs,
 * and long is the number of inputs. OutputT will be double, because it represents the average of the inputs.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface Accumulation<InputT, AccumT, OutputT>
{
  /**
   * Returns the default accumulated value when nothing has been accumulated
   *
   * @return the default accumulated value
   */
  AccumT defaultAccumulatedValue();

  /**
   * Accumulates the input to the accumulated value
   *
   * @param accumulatedValue the accumulated value
   * @param input the input value
   * @return the result accumulated value
   */
  AccumT accumulate(AccumT accumulatedValue, InputT input);

  /**
   * Merges two accumulated values into one
   *
   * @param accumulatedValue1 the first accumulated value
   * @param accumulatedValue2 the second accumulated value
   * @return the result accumulated value
   */
  AccumT merge(AccumT accumulatedValue1, AccumT accumulatedValue2);

  /**
   * Gets the output of the accumulated value. This is used for generating the data for triggers
   *
   * @param accumulatedValue the accumulated value
   * @return the output
   */
  OutputT getOutput(AccumT accumulatedValue);

  /**
   * Gets the retraction of the value. This is used for retracting previous panes in
   * ACCUMULATING_AND_RETRACTING accumulation mode
   *
   * @param value the value to be retracted
   * @return the retracted value
   */
  OutputT getRetraction(OutputT value);
}
