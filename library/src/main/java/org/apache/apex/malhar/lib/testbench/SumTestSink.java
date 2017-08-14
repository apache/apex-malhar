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
package org.apache.apex.malhar.lib.testbench;

import com.datatorrent.api.Sink;

/**
 * A sink implementation which collects Number tuples and sums their values.
 * <p></p>
 * @displayName Sum Test Sink
 * @category Test Bench
 * @tags numeric
 * @since 0.3.2
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
      val += ((Number)payload).doubleValue();
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
