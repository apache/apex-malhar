/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.benchmarks;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class LongAggregator implements Operator
{
  private Long sum = 0L;

  public transient final DefaultInputPort<Long[]> input = new DefaultInputPort<Long[]>() {
    @Override
    public void process(Long[] tuple)
    {
      for(int index = 0;
          index < tuple.length;
          index++) {
        sum += tuple[index];
      }
    }
  };

  public LongAggregator()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }
}
