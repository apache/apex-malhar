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
package org.apache.apex.malhar.lib.util;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;

/**
 * This unifier consumes numbers, and emits their sum at the end of each application window.
 * <p>
 * This unifier uses round robin partitioning.
 * </p>
 * @displayName Unifier Sum Number
 * @category Algorithmic
 * @tags numeric
 * @since 0.3.2
 */
public class UnifierSumNumber<V extends Number> extends BaseNumberValueOperator<V> implements Unifier<V>
{
  private Double result = 0.0;
  private boolean doEmit = false;
  /**
   * This is the output port which emits a sum.
   */
  public final transient DefaultOutputPort<V> mergedport = new DefaultOutputPort<V>();

  /**
   * Adds tuple with result so far
   *
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(V tuple)
  {
    result += tuple.doubleValue();
    doEmit = true;
  }

  /**
   * emits the result, and resets it
   */
  @Override
  public void endWindow()
  {
    if (doEmit) {
      mergedport.emit(getValue(result));
      result = 0.0;
      doEmit = false;
    }
  }

  /**
   * a no-op
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * a noop
   */
  @Override
  public void teardown()
  {
  }
}
