/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

/**
 *
 * Combiner for an output port that emits object with <V> interface and has the processing done
 * with round robin partitioning. The final tuple is sum of all partition values
 *
 * @author amol<br>
 *
 */
public class UnifierSumNumber<V extends Number> extends BaseNumberValueOperator<V> implements Unifier<V>
{
  private Double result = 0.0;
  private boolean doEmit = false;
  public final transient DefaultOutputPort<V> mergedport = new DefaultOutputPort<V>(this);

  /**
   * Adds tuple with result so far
   *
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(V tuple)
  {
    result += tuple.doubleValue();
    doEmit = true;
  }

  /**
   * emits the result, and resets it
   */
  @Override
  public void endWindow()
  {
    if (doEmit) {
      mergedport.emit(getValue(result));
      result = 0.0;
      doEmit = false;
    }
  }

  /**
   * a no-op
   *
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
