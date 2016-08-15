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
 * This interface describes the individual window.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface Window<WINDOW extends Window> extends Comparable<WINDOW>
{
  long getBeginTimestamp();

  long getDurationMillis();

  /**
   * Global window means there is only one window, or no window depending on how you look at it.
   */
  class GlobalWindow implements Window<GlobalWindow>
  {

    /**
     * The singleton global window
     */
    private static GlobalWindow GLOBAL_WINDOW = new GlobalWindow();

    private GlobalWindow()
    {
    }

    public static GlobalWindow getInstance()
    {
      return GLOBAL_WINDOW;
    }

    @Override
    public long getBeginTimestamp()
    {
      return 0;
    }

    public long getDurationMillis()
    {
      return Long.MAX_VALUE;
    }

    @Override
    public boolean equals(Object other)
    {
      return (other instanceof GlobalWindow);
    }

    @Override
    public int compareTo(GlobalWindow o)
    {
      return 0;
    }
  }

  /**
   * TimeWindow is a window that represents a time slice
   */
  class TimeWindow<WINDOW extends TimeWindow> implements Window<WINDOW>
  {
    protected final long beginTimestamp;
    protected final long durationMillis;

    private TimeWindow()
    {
      // for kryo
      this.beginTimestamp = -1;
      this.durationMillis = 0;
    }

    public TimeWindow(long beginTimestamp, long durationMillis)
    {
      this.beginTimestamp = beginTimestamp;
      this.durationMillis = durationMillis;
    }

    /**
     * Gets the beginning timestamp of this window
     *
     * @return
     */
    @Override
    public long getBeginTimestamp()
    {
      return beginTimestamp;
    }

    /**
     * Gets the duration millis of this window
     *
     * @return
     */
    @Override
    public long getDurationMillis()
    {
      return durationMillis;
    }

    @Override
    public boolean equals(Object other)
    {
      if (other instanceof TimeWindow) {
        TimeWindow otherWindow = (TimeWindow)other;
        return this.beginTimestamp == otherWindow.beginTimestamp && this.durationMillis == otherWindow.durationMillis;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode()
    {
      return (int)(beginTimestamp & 0xffffffffL);
    }

    @Override
    public int compareTo(TimeWindow o)
    {
      if (this.getBeginTimestamp() < o.getBeginTimestamp()) {
        return -1;
      } else if (this.getBeginTimestamp() > o.getBeginTimestamp()) {
        return 1;
      } else if (this.getDurationMillis() < o.getDurationMillis()) {
        return -1;
      } else if (this.getDurationMillis() > o.getDurationMillis()) {
        return 1;
      }
      return 0;
    }
  }

  /**
   * SessionWindow is a window that represents a time slice for a key, with the time slice being variable length.
   *
   * @param <K>
   */
  class SessionWindow<K> extends TimeWindow<SessionWindow<K>>
  {
    private final K key;

    private SessionWindow()
    {
      // for kryo
      this.key = null;
    }

    public SessionWindow(K key, long beginTimestamp, long duration)
    {
      super(beginTimestamp, duration);
      this.key = key;
    }

    public K getKey()
    {
      return key;
    }

    @Override
    public boolean equals(Object other)
    {
      if (!super.equals(other)) {
        return false;
      }
      if (other instanceof SessionWindow) {
        SessionWindow<K> otherSessionWindow = (SessionWindow<K>)other;
        if (key == null) {
          return otherSessionWindow.key == null;
        } else {
          return key.equals(otherSessionWindow.key);
        }
      } else {
        return false;
      }
    }

    @Override
    public int hashCode()
    {
      return (int)((key.hashCode() << 16) | (beginTimestamp & 0xffffL));
    }

    @Override
    public int compareTo(SessionWindow<K> o)
    {
      int val = super.compareTo(o);
      if (val == 0) {
        if (this.getKey() instanceof Comparable) {
          return ((Comparable<K>)this.getKey()).compareTo(o.getKey());
        } else {
          return Long.compare(this.getKey().hashCode(), o.getKey().hashCode());
        }
      } else {
        return val;
      }
    }
  }
}
