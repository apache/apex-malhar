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
 * This unifier consumes boolean tuples.&nbsp;
 * All the tuples received within an application window are ANDED together and the result is emitted at the end of the window.
 * <p>
 * The processing is done with sticky key partitioning, i.e. each one key belongs only to one partition.
 * </p>
 * @displayName Unifier Boolean And
 * @category Algorithmic
 * @tags unifier, and
 * @since 0.3.2
 */
public class UnifierBooleanAnd implements Unifier<Boolean>
{
  boolean result = true;
  boolean doemit = false;

  /**
   * This is the output port which emits the ANDED input tuples result.
   */
  public final transient DefaultOutputPort<Boolean> mergedport = new DefaultOutputPort<Boolean>();

  /**
   * ANDs tuple with result so far
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(Boolean tuple)
  {
    doemit = true;
    result = tuple && result;
  }

  /**
   * resets flag to true
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    result = true;
    doemit = false;
  }

  /**
   * emits the result
   */
  @Override
  public void endWindow()
  {
    if (doemit) {
      mergedport.emit(result);
    }
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
