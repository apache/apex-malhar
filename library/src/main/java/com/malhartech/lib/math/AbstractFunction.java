/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 * Calculus Operator for which operates on a single variable input and produces a result.
 * If the equation looks like
 * y = f(x)
 * For this operator x is the input and y is the output.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractFunction extends BaseOperator
{
  public final transient DefaultOutputPort<Double> doubleResult = new DefaultOutputPort<Double>(this);
  public final transient DefaultOutputPort<Float> floatResult = new DefaultOutputPort<Float>(this);
  public final transient DefaultOutputPort<Long> longResult = new DefaultOutputPort<Long>(this);
  public final transient DefaultOutputPort<Integer> integerResult = new DefaultOutputPort<Integer>(this);
}
