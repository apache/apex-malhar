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

import org.joda.time.Duration;

import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Function;

/**
 * This interface describes what needs to be implemented for the operator that supports the Apache Beam model of
 * windowing and triggering
 *
 * TODO: We may not need this interface at all since there are no components that make use of these methods generically.
 * TODO: We may wanna just use the abstract class {@link org.apache.apex.malhar.lib.window.impl.AbstractWindowedOperator}
 *
 * @param <InputT> The type of the input tuple
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface WindowedOperator<InputT>
{

  /**
   * Sets the WindowOption of this operator
   *
   * @param windowOption the window option
   */
  void setWindowOption(WindowOption windowOption);

  /**
   * Sets the TriggerOption of this operator
   *
   * @param triggerOption the trigger option
   */
  void setTriggerOption(TriggerOption triggerOption);

  /**
   * Sets the allowed lateness of this operator
   *
   * @param allowedLateness the allowed lateness
   */
  void setAllowedLateness(Duration allowedLateness);

  /**
   * This sets the function that extracts the timestamp from the input tuple
   *
   * @param timestampExtractor the timestamp extractor
   */
  void setTimestampExtractor(Function<InputT, Long> timestampExtractor);

  /**
   * Assign window(s) for this input tuple
   *
   * @param input the input tuple
   * @return the windowed tuple
   */
  Tuple.WindowedTuple<InputT> getWindowedValue(Tuple<InputT> input);

  /**
   * This method returns whether the given timestamp is too late for processing.
   * The implementation of this operator should look at the allowed lateness in the WindowOption.
   * It should also call this function and if it returns true, it should drop the associated tuple.
   *
   * @param timestamp the timestamp
   * @return whether the timestamp is considered too late
   */
  boolean isTooLate(long timestamp);

  /**
   * This method is supposed to drop the tuple because it has passed the allowed lateness. But an implementation
   * of this method has the chance to do something different (e.g. emit it to another port)
   *
   * @param input the input tuple
   */
  void dropTuple(Tuple<InputT> input);

  /**
   * This method accumulates the incoming tuple (with the Accumulation interface)
   *
   * @param tuple the input tuple
   */
  void accumulateTuple(Tuple.WindowedTuple<InputT> tuple);

  /**
   * This method should be called when the watermark for the given timestamp arrives
   * The implementation should retrieve all valid windows in its state that lies completely before this watermark,
   * and change the state of each of those windows. All tuples for those windows arriving after
   * the watermark will be considered late.
   *
   * @param watermark the watermark tuple
   */
  void processWatermark(ControlTuple.Watermark watermark);

  /**
   * This method fires the trigger for the given window, and possibly retraction trigger. The implementation should clear
   * the window data in the storage if the accumulation mode is DISCARDING
   *
   * @param window the window
   * @param windowState the window state
   */
  void fireTrigger(Window window, WindowState windowState);

  /**
   * This method clears the window data in the storage.
   *
   * @param window the window
   */
  void clearWindowData(Window window);

}
