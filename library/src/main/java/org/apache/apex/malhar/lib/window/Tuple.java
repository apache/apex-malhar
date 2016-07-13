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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * All tuples that use the WindowedOperator must be an implementation of this interface
 */
@InterfaceStability.Evolving
public interface Tuple<T>
{
  /**
   * Gets the value of the tuple
   *
   * @return
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
  }

  /**
   * Tuple that is wrapped by a timestamp and one or more windows
   *
   * @param <T>
   */
  class WindowedTuple<T> extends TimestampedTuple<T>
  {
    private List<Window> windows = new ArrayList<>();

    public WindowedTuple()
    {
    }

    public WindowedTuple(Window window, T value)
    {
      super(window.getBeginTimestamp(), value);
      this.windows.add(window);
    }

    public List<Window> getWindows()
    {
      return windows;
    }

    public void addWindow(Window window)
    {
      this.windows.add(window);
    }
  }

}
