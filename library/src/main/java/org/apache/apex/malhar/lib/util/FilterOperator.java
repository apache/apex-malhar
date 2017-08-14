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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is the base implementation of an operator, which consumes tuples.&nbsp;
 * If the tuples satisfy a specified filtering function, then they are emitted.&nbsp;
 * Subclasses should implement the filtering method.
 * <p></p>
 * @displayName Filter
 * @category Algorithmic
 * @tags filter
 * @since 0.3.4
 */
public abstract class FilterOperator extends BaseOperator
{
  /**
   * This is the input port on which tuples are received.
   */
  @InputPortFieldAnnotation(optional = false)
  public final transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      if (satisfiesFilter(tuple)) {
        out.emit(tuple);
      }
    }

  };

  /**
   * This is the output port, which emits tuples that satisfy the filter.
   */
  @OutputPortFieldAnnotation(optional = false)
  public final transient DefaultOutputPort<Object> out = new DefaultOutputPort<Object>();

  public abstract boolean satisfiesFilter(Object tuple);
}
