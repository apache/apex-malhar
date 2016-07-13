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
package org.apache.apex.malhar.lib.dedup;

import java.util.List;

import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * Accumulation for Dedup operator will maintain a list of events for each event window
 *
 * @param <T>
 */
public class DedupAccumulation<T> implements Accumulation<T, List<T>, T>
{

  @Override
  public List<T> defaultAccumulatedValue()
  {
    return null;
  }

  /**
   * return the list of tuples in the event window
   */
  @Override
  public List<T> accumulate(List<T> accumulatedValue, T input)
  {
    accumulatedValue.add(input);
    return accumulatedValue;
  }

  @Override
  public List<T> merge(List<T> accumulatedValue1, List<T> accumulatedValue2)
  {
    throw new UnsupportedOperationException("merge() not needed for dedup");
  }

  @Override
  public T getOutput(List<T> accumulatedValue)
  {
    throw new UnsupportedOperationException("getOutput() not needed for dedup");
  }

  @Override
  public T getRetraction(T accumulatedValue)
  {
    throw new UnsupportedOperationException("getRetraction() not needed for Dedup");
  }
  
}
