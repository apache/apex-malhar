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

/**
 * Accumulation that does a simple sum of longs
 */
public class SumAccumulation implements Accumulation<Long, Long, Long>
{
  @Override
  public Long defaultAccumulatedValue()
  {
    return 0L;
  }

  @Override
  public Long accumulate(Long accumulatedValue, Long input)
  {
    return accumulatedValue + input;
  }

  @Override
  public Long merge(Long accumulatedValue1, Long accumulatedValue2)
  {
    return accumulatedValue1 + accumulatedValue2;
  }

  @Override
  public Long getOutput(Long accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public Long getRetraction(Long accumulatedValue)
  {
    return -accumulatedValue;
  }
}
