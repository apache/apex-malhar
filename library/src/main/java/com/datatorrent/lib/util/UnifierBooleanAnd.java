/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

/**
 *
 * Combiner for an output port that emits object with Map<K,V> interface and has the processing done
 * with sticky key partition, i.e. each one key belongs only to one partition. The final output of the
 * combiner is a simple merge into a single object that implements Map
 *
 * @since 0.3.2
 */
public class UnifierBooleanAnd implements Unifier<Boolean>
{
  boolean result = true;
  boolean doemit = false;
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
