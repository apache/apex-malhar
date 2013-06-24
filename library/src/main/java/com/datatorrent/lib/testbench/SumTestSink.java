/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.testbench;

import com.datatorrent.api.Sink;

/**
 * A sink implementation to collect expected test results.
 */
public class SumTestSink<T> implements Sink<T>
{
  public Double val = 0.0;

  public void clear()
  {
    val = 0.0;
  }

 /**
   *
   * @param payload
   */
  @Override
  public void put(T payload)
  {
    if (payload instanceof Number) {
      val += ((Number) payload).doubleValue();
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
