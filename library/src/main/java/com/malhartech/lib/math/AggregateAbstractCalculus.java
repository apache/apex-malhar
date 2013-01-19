/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.DefaultInputPort;
import java.util.Collection;

/**
 *
 * @param <T> 
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AggregateAbstractCalculus<T extends Number> extends AbstractFunction
{
  public final transient DefaultInputPort<Collection<T>> input = new DefaultInputPort<Collection<T>>(this)
  {
    @Override
    public void process(Collection<T> collection)
    {
      Double dResult = null;
      if (doubleResult.isConnected()) {
        doubleResult.emit(dResult = aggregateDoubles(collection));
      }

      if (floatResult.isConnected()) {
        floatResult.emit(dResult == null ? (float)(aggregateDoubles(collection)) : dResult.floatValue());
      }

      Long lResult = null;
      if (longResult.isConnected()) {
        longResult.emit(lResult = aggregateLongs(collection));
      }

      if (integerResult.isConnected()) {
        integerResult.emit(lResult == null ? (int)aggregateLongs(collection) : lResult.intValue());
      }
    }

  };

  public abstract long aggregateLongs(Collection<T> collection);

  public abstract double aggregateDoubles(Collection<T> collection);

}
