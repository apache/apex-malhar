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
package org.apache.apex.malhar.lib.util.time;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;

/**
 * This a is class which holds utility functions that can be used by operators to perform common
 * tasks.
 *
 * @since 3.3.0
 */
public class WindowUtils
{
  /**
   * Computes the duration of an Operator's application window in milliseconds.
   * @param context The context of the Operator whose application window duration is
   * being computed.
   * @return The duration of an Operator's application window in milliseconds.
   */
  public static long getAppWindowDurationMs(OperatorContext context)
  {
    return context.getValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS).longValue() *
           context.getValue(OperatorContext.APPLICATION_WINDOW_COUNT).longValue();
  }

  /**
   * Computes the number of application windows that will fit into the given
   * millisecond duration. This method rounds the number of application windows up.
   * @param context The context of the Operator for which the number of application
   * windows is being computed.
   * @param millis The millisecond duration to compute the number of application windows for.
   * @return The number of application windows that will fit into the given millisecond duration.
   */
  public static long msToAppWindowCount(OperatorContext context, long millis)
  {
    Preconditions.checkArgument(millis > 0);
    long appWindowDurationMS = getAppWindowDurationMs(context);
    long appWindowCount = millis / appWindowDurationMS;

    if (millis % appWindowDurationMS != 0) {
      appWindowCount++;
    }

    return appWindowCount;
  }

  /**
   * Converts tuples per second into tuples per application window. The value for
   * tuples per application window is rounded up.
   * @param context The context of the Operator for which tuples per application window is
   * being computed.
   * @param tuplesPerSecond Tuples per second.
   * @return Tuples per application window.
   */
  public static long tpsToTpw(OperatorContext context, long tuplesPerSecond)
  {
    Preconditions.checkArgument(tuplesPerSecond > 0);
    BigDecimal tuplesPerWindow = new BigDecimal(getAppWindowDurationMs(context));
    tuplesPerWindow = tuplesPerWindow.divide(new BigDecimal(1000));
    tuplesPerWindow = tuplesPerWindow.multiply(new BigDecimal(tuplesPerSecond));

    Preconditions.checkArgument(tuplesPerWindow.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0,
        "Overflow computing tuples per window.");

    tuplesPerWindow = tuplesPerWindow.stripTrailingZeros();
    long tuplesPerWindowLong = tuplesPerWindow.longValue();

    if (tuplesPerWindow.scale() > 0) {
      LOG.debug("{}", tuplesPerWindow);
      tuplesPerWindowLong++;
    }

    return tuplesPerWindowLong;
  }

  private static final Logger LOG = LoggerFactory.getLogger(WindowUtils.class);
}
