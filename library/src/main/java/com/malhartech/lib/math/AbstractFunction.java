/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;

/**
 * Calculus Operator for which operates on a single variable input and produces a result.
 * If the equation looks like
 * y = f(x)
 * For this operator x is the input and y is the output.
 * Ports are optional
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractFunction extends BaseOperator
{
  @OutputPortFieldAnnotation(name = "doubleResult", optional = true)
  public final transient DefaultOutputPort<Double> doubleResult = new DefaultOutputPort<Double>(this);


  @OutputPortFieldAnnotation(name = "floatResult", optional = true)
  public final transient DefaultOutputPort<Float> floatResult = new DefaultOutputPort<Float>(this);

  @OutputPortFieldAnnotation(name = "longResult", optional = true)
  public final transient DefaultOutputPort<Long> longResult = new DefaultOutputPort<Long>(this);

  @OutputPortFieldAnnotation(name = "integerResult", optional = true)
  public final transient DefaultOutputPort<Integer> integerResult = new DefaultOutputPort<Integer>(this);
}
