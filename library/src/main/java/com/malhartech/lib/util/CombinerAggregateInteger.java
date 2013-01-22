/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;
import java.util.HashMap;

/**
 *
 * Combiner for an output port that emits object with Map<K,V> interface and has the processing done
 * with sticky key partition, i.e. each one key belongs only to one partition. The final output of the
 * combiner is a simple merge into a single object that implements Map
 *
 * @author amol<br>
 *
 */
public class CombinerAggregateInteger implements Unifier<Integer>
{
  Integer result = 0;
  public final transient DefaultOutputPort<Integer> mergedport = new DefaultOutputPort<Integer>(this);

  /**
   * Adds tuple with result so far
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void merge(Integer tuple)
  {
    result = result + tuple;
  }

  /**
   * no-op
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
    mergedport.emit(result);
    result = 0;
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
