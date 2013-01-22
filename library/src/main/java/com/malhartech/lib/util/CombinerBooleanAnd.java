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
public class CombinerBooleanAnd implements Unifier<Boolean>
{
  boolean result = true;
  public final transient DefaultOutputPort<Boolean> mergedport = new DefaultOutputPort<Boolean>(this);

  /**
   * ANDs tuple with result so far
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void merge(Boolean tuple)
  {
    result = tuple && result;
  }

  /**
   * resets flag to true
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    result = true;
  }

  /**
   * emits the result
   */
  @Override
  public void endWindow()
  {
    mergedport.emit(result);
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
