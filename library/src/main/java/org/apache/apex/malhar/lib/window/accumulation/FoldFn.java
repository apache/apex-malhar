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
 * Fold Accumulation Adaptor class
 *
 * @since 3.5.0
 */
public abstract class FoldFn<INPUT, OUTPUT> implements Accumulation<INPUT, OUTPUT, OUTPUT>
{

  public FoldFn()
  {
  }

  public FoldFn(OUTPUT initialVal)
  {
    this.initialVal = initialVal;
  }

  private OUTPUT initialVal;

  @Override
  public OUTPUT defaultAccumulatedValue()
  {
    return initialVal;
  }

  @Override
  public OUTPUT accumulate(OUTPUT accumulatedValue, INPUT input)
  {
    return fold(accumulatedValue, input);
  }

  @Override
  public OUTPUT getOutput(OUTPUT accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public OUTPUT getRetraction(OUTPUT value)
  {
    return null;
  }

  abstract OUTPUT fold(OUTPUT result, INPUT input);
}
