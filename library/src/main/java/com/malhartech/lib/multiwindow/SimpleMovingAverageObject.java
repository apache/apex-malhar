/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.multiwindow;

import com.malhartech.lib.util.MutableDouble;
import com.malhartech.lib.util.MutableInteger;

/**
 * Information needed to calculate simple moving average.
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class SimpleMovingAverageObject implements SlidingWindowObject
{
  private MutableDouble sum;
  private MutableInteger count;

  public double getSum()
  {
    return sum.value;
  }

  public int getCount()
  {
    return count.value;
  }

  public SimpleMovingAverageObject()
  {
    sum = new MutableDouble(0);
    count = new MutableInteger(0);
  }

  public void add(double d)
  {
    sum.add(d);
    count.add(1);
  }

  @Override
  public void clear()
  {
    sum.value = 0.0;
    count.value = 0;
  }
}
