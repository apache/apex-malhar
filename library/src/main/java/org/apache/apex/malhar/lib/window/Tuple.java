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

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * All tuples that use the WindowedOperator must be an implementation of this interface
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface Tuple<T>
{
  /**
   * Gets the value of the tuple
   *
   * @return the value
   */
  T getValue();

  /**
   * Plain tuple class
   *
   * @param <T>
   */
  class PlainTuple<T> implements Tuple<T>
  {
    private T value;

    private PlainTuple()
    {
      // for kryo
    }

    public PlainTuple(T value)
    {
      this.value = value;
    }

    public T getValue()
    {
      return value;
    }

    public void setValue(T value)
    {
      this.value = value;
    }

    @Override
    public String toString()
    {
      return value.toString();
    }

    @Override
    public int hashCode()
    {
      return value.hashCode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj)
    {
      if (obj instanceof PlainTuple) {
        PlainTuple<T> other = (PlainTuple<T>)obj;
        if (this.value == null) {
          return other.value == null;
        } else {
          return this.value.equals(other.value);
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Tuple that is wrapped by a timestamp
   *
   * @param <T>
   */
  class TimestampedTuple<T> extends PlainTuple<T>
  {
    private long timestamp;

    private TimestampedTuple()
    {
      // for kryo
    }

    public TimestampedTuple(long timestamp, T value)
    {
      super(value);
      this.timestamp = timestamp;
    }

    public long getTimestamp()
    {
      return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
      this.timestamp = timestamp;
    }

    @Override
    public int hashCode()
    {
      return super.hashCode() ^ (int)(timestamp & 0xffffff);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj)
    {
      if (obj instanceof TimestampedTuple && super.equals(obj)) {
        TimestampedTuple<T> other = (TimestampedTuple<T>)obj;
        return (this.timestamp == other.timestamp);
      } else {
        return false;
      }
    }
  }

  /**
   * Tuple that is wrapped by a timestamp and one or more windows
   *
   * @param <T>
   */
  class WindowedTuple<T> extends TimestampedTuple<T>
  {
    private Set<Window> windows = new TreeSet<>();

    public WindowedTuple()
    {
    }

    public WindowedTuple(Window window, T value)
    {
      super(window.getBeginTimestamp(), value);
      this.windows.add(window);
    }

    public WindowedTuple(Collection<? extends Window> windows, long timestamp, T value)
    {
      super(timestamp, value);
      this.windows.addAll(windows);
    }

    public Collection<Window> getWindows()
    {
      return windows;
    }

    public void addWindow(Window window)
    {
      this.windows.add(window);
    }

    @Override
    public int hashCode()
    {
      return super.hashCode() ^ windows.hashCode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj)
    {
      if (obj instanceof WindowedTuple && super.equals(obj)) {
        WindowedTuple<T> other = (WindowedTuple<T>)obj;
        return this.windows.equals(other.windows);
      } else {
        return false;
      }
    }
  }

}
