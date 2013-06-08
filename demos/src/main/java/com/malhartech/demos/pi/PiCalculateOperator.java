/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class PiCalculateOperator extends BaseOperator
{
  private transient int x = -1;
  private transient int y = -1;
  private int base;
  private transient double pi;
  private long inArea = 0;
  private long totalArea = 0;
  public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>(this)
  {
    @Override
    public void process(Integer tuple)
    {
      if (x == -1) {
        x = tuple;
      }
      else {
        y = tuple;
        if (x * x + y * y <= base) {
          inArea++;
        }
        totalArea++;
        x = y = -1;
      }
    }

  };
  public final transient DefaultOutputPort<Double> output = new DefaultOutputPort<Double>(this);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    pi = (double)inArea / totalArea * 4;
    output.emit(pi);
  }

  public void setBase(int num)
  {
    base = num;
  }

}
