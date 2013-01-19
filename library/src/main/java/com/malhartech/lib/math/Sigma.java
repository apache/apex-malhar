/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import java.util.Collection;

/**
 *
 * @param <T> 
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Sigma<T extends Number> extends AggregateAbstractCalculus<T>
{
  @Override
  public long aggregateLongs(Collection<T> collection)
  {
    long l = 0;

    for (Number n: collection) {
      l += n.longValue();
    }

    return l;
  }

  @Override
  public double aggregateDoubles(Collection<T> collection)
  {
    double d = 0;

    for (Number n: collection) {
      d += n.doubleValue();
    }

    return d;
  }

}
