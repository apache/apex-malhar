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
package org.apache.apex.malhar.lib.math;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * Abstract base operator defining optional double/float/long/integer output port.
 *  <p>
 *  Common port declaration in operators.
 *
 * @displayName Abstract Output
 * @category Math
 * @tags output operator, multiple datatype
 * @since 0.3.3
 */
public abstract class AbstractOutput extends BaseOperator
{
  /**
   * Double type output.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Double> doubleResult = new DefaultOutputPort<Double>();

  /**
   * Float type output.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Float> floatResult = new DefaultOutputPort<Float>();

  /**
   * Long type output.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Long> longResult = new DefaultOutputPort<Long>();

  /**
   * Integer type output.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Integer> integerResult = new DefaultOutputPort<Integer>();
}
