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
 * This unifier consumes numbers, and emits the maximum and minimum tuples at the end of each window.
 * <p>
 * This unifier uses round robin partitioning.
 * </p>
 * @displayName Unifier Range
 * @category Algorithmic
 * @tags numeric
 * @since 0.3.2
 */
public class UnifierRange<V extends Number> implements Unifier<HighLow<V>>
{
  public HighLow<V> mergedTuple = null;

  /**
   * This is the output port which emits the minimum and maximum.
   */
  public final transient DefaultOutputPort<HighLow<V>> mergedport = new DefaultOutputPort<HighLow<V>>();

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(HighLow<V> tuple)
  {
    if (mergedTuple == null) {
      mergedTuple = new HighLow(tuple.getHigh(), tuple.getLow());
    } else {
      if (mergedTuple.getHigh().doubleValue() < tuple.getHigh().doubleValue()) {
        mergedTuple.setHigh(tuple.getHigh());
      }
      if (mergedTuple.getLow().doubleValue() > tuple.getLow().doubleValue()) {
        mergedTuple.setLow(tuple.getLow());
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    if (mergedTuple != null) {
      mergedport.emit(mergedTuple);
      mergedTuple = null;
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }
}
