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

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

/**
 * This class describes how windowing is done
 *
 *  This is used by both the high level API and by the WindowedOperator
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface WindowOption
{
  /**
   * The windowing specification that says there is only one window for the entire time of the application
   */
  class GlobalWindow implements WindowOption
  {
  }

  /**
   * The windowing specification that divides the time into slices with the same width
   */
  class TimeWindows implements WindowOption
  {
    @FieldSerializer.Bind(JavaSerializer.class)
    private Duration duration;

    private TimeWindows()
    {
      // for kryo
    }

    public TimeWindows(Duration duration)
    {
      this.duration = duration;
    }

    /**
     * Gets the duration of the time window
     *
     * @return the duration of the time window
     */
    public Duration getDuration()
    {
      return duration;
    }

    /**
     * The time window should be a sliding window with the given slide duration
     *
     * @param duration the slide by duration
     * @return the SlidingTimeWindows
     */
    public SlidingTimeWindows slideBy(Duration duration)
    {
      return new SlidingTimeWindows(this.duration, duration);
    }
  }

  /**
   * The window specification that represents sliding windows
   *
   */
  class SlidingTimeWindows extends TimeWindows
  {
    @FieldSerializer.Bind(JavaSerializer.class)
    private Duration slideByDuration;

    private SlidingTimeWindows()
    {
      // for kryo
    }

    public SlidingTimeWindows(Duration size, Duration slideByDuration)
    {
      super(size);
      if (size.getMillis() % slideByDuration.getMillis() != 0) {
        throw new IllegalArgumentException("Window size must be divisible by the slide-by duration");
      }
      this.slideByDuration = slideByDuration;
    }

    public Duration getSlideByDuration()
    {
      return slideByDuration;
    }
  }

  /**
   * The window specification that represents session windows, with a minimum gap duration between two tuples with the same key in two different session windows. The minimum gap is also the minimum duration of any session window.
   */
  class SessionWindows implements WindowOption
  {
    @FieldSerializer.Bind(JavaSerializer.class)
    private Duration minGap;

    private SessionWindows()
    {
      // for kryo
    }

    public SessionWindows(Duration minGap)
    {
      this.minGap = minGap;
    }

    public Duration getMinGap()
    {
      return minGap;
    }
  }

}
