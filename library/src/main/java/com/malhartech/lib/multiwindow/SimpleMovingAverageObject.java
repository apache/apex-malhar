/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.multiwindow;

import com.malhartech.lib.util.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Information needed to calculate simple moving average.
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class SimpleMovingAverageObject implements SlidingWindowObject
{
  private MutableDouble sum;
  private MutableInt count;

  public double getSum()
  {
    return sum.value;
  }

  public int getCount()
  {
    return count.intValue();
  }

  public SimpleMovingAverageObject()
  {
    sum = new MutableDouble(0);
    count = new MutableInt(0);
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
    count.setValue(0);
  }
}
