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
 * This interface describes the classes that represent individual windows.
 *
 * @param <WINDOW> window type the object of this class can call compareTo
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface Window<WINDOW extends Comparable<WINDOW>> extends Comparable<WINDOW>
{
  long getBeginTimestamp();

  long getDurationMillis();

  /**
   * Global window means there is only one window that spans the entire life time of the application
   */
  class GlobalWindow implements Window<GlobalWindow>
  {

    /**
     * The singleton global window
     */
    public static final GlobalWindow INSTANCE = new GlobalWindow();

    private GlobalWindow()
    {
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
      return this == other;
    }

    @Override
    public int compareTo(GlobalWindow o)
    {
      return 0;
    }

    @Override
    public String toString()
    {
      return "[GlobalWindow]";
    }
  }

  /**
   * TimeWindow is a window that represents a time slice
   *
   * @param <WINDOW> window type the object of this class can call compareTo
   */
  class TimeWindow<WINDOW extends TimeWindow<WINDOW>> implements Window<WINDOW>
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
     * @return the begin timestamp
     */
    @Override
    public long getBeginTimestamp()
    {
      return beginTimestamp;
    }

    /**
     * Gets the duration millis of this window
     *
     * @return the duration
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
      int result = (int)(beginTimestamp ^ (beginTimestamp >>> 32));
      result = 31 * result + (int)(durationMillis ^ (durationMillis >>> 32));
      return result;
    }

    @Override
    public int compareTo(TimeWindow o)
    {
      long diff = (this.getBeginTimestamp() + this.getDurationMillis()) - (o.getBeginTimestamp() + o.getDurationMillis());
      if (diff == 0) {
        return Long.signum(this.getBeginTimestamp() - o.getBeginTimestamp());
      } else {
        return Long.signum(diff);
      }
    }

    @Override
    public String toString()
    {
      return "[TimeWindow " + getBeginTimestamp() + "(" + getDurationMillis() + ")]";
    }
  }

  /**
   * SessionWindow is a window that represents a time slice for a key, with the time slice being variable length.
   *
   * @param <K> the key type for the session
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
        @SuppressWarnings("unchecked")
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
      return (key.hashCode() << 16) | super.hashCode();
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

    @Override
    public String toString()
    {
      return "[SessionWindow key=" + getKey() + " " + getBeginTimestamp() + "(" + getDurationMillis() + ")]";
    }
  }
}
