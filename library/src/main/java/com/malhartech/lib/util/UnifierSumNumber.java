/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;

/**
 *
 * Combiner for an output port that emits object with Map<K,V> interface and has the processing done
 * with sticky key partition, i.e. each one key belongs only to one partition. The final output of the
 * combiner is a simple merge into a single object that implements Map
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
  public void merge(V tuple)
  {
    result += tuple.doubleValue();
    doEmit = true;
  }

  /**
   * no-op
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
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
