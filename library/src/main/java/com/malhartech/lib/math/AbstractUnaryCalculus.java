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
 *
 * @param <T> Type of the input value
 * @param <U> Type of the output value
 */
public abstract class AbstractUnaryCalculus<T, U> extends BaseOperator
{
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      output.emit(AbstractUnaryCalculus.this.function(tuple));
    }

  };
  public final transient DefaultOutputPort<U> output = new DefaultOutputPort<U>(this);

  /**
   * Transform the input into the output after applying appropriate mathematical function to it.
   *
   * @param inputNumber argument to the function
   * @return result of the function
   */
  public abstract U function(T inputNumber);

}
