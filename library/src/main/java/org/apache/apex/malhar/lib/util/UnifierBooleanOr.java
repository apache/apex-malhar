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
 *
 * This unifier consumes boolean tuples.&nbsp;
 * All the tuples received within an application window are ORED together and the result is emitted at the end of the window.
 * <p>
 * The processing is done with sticky key partitioning, i.e. each one key belongs only to one partition.
 * </p>
 * @displayName Unifier Boolean Or
 * @category Algorithmic
 * @tags unifier, or
 * @since 0.3.2
 */
public class UnifierBooleanOr implements Unifier<Boolean>
{
  boolean result = false;
  /**
   * This is the output port which emits the ORED tuple result.
   */
  public final transient DefaultOutputPort<Boolean> mergedport = new DefaultOutputPort<Boolean>();

  /**
   * emits result if any partition returns true. Then on the window emits no more tuples
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(Boolean tuple)
  {
    if (!result) {
      if (tuple) {
        mergedport.emit(true);
      }
      result = true;
    }
  }

  /**
   * resets flag
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    result = false;
  }

  /**
   * noop
   */
  @Override
  public void endWindow()
  {
  }

  /**
   * a no-op
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
