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
package org.apache.apex.malhar.lib.window.accumulation;

import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * An easy to use reduce Accumulation
 * @param <INPUT>
 *
 * @since 3.5.0
 */
public abstract class ReduceFn<INPUT> implements Accumulation<INPUT, INPUT, INPUT>
{
  @Override
  public INPUT defaultAccumulatedValue()
  {
    return null;
  }

  @Override
  public INPUT accumulate(INPUT accumulatedValue, INPUT input)
  {
    if (accumulatedValue == null) {
      return input;
    }
    return reduce(accumulatedValue, input);
  }

  @Override
  public INPUT merge(INPUT accumulatedValue1, INPUT accumulatedValue2)
  {
    return reduce(accumulatedValue1, accumulatedValue2);
  }

  @Override
  public INPUT getOutput(INPUT accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public INPUT getRetraction(INPUT value)
  {
    return null;
  }

  public abstract INPUT reduce(INPUT input1, INPUT input2);


}
