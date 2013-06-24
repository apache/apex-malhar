/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Calculus Operator for which operates on a single variable input and produces a result.
 * If the equation looks like
 * y = f(x)
 * For this operator x is the input and y is the output.
 * Ports are optional
 *
 */
public abstract class AbstractFunction extends BaseOperator
{
  @OutputPortFieldAnnotation(name = "doubleResult", optional = true)
  public final transient DefaultOutputPort<Double> doubleResult = new DefaultOutputPort<Double>();


  @OutputPortFieldAnnotation(name = "floatResult", optional = true)
  public final transient DefaultOutputPort<Float> floatResult = new DefaultOutputPort<Float>();

  @OutputPortFieldAnnotation(name = "longResult", optional = true)
  public final transient DefaultOutputPort<Long> longResult = new DefaultOutputPort<Long>();

  @OutputPortFieldAnnotation(name = "integerResult", optional = true)
  public final transient DefaultOutputPort<Integer> integerResult = new DefaultOutputPort<Integer>();
}
