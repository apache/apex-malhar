/**
/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.testbench;

import com.malhartech.api.Sink;

/**
 * A sink implementation to collect expected test results.
 */
public class SumTestSink<T> implements Sink<T>
{
  public Double val = 0.0;

  public void clear()
  {
    val = 0.0;
  }

 /**
   *
   * @param payload
   */
  @Override
  public void put(T payload)
  {
    if (payload instanceof Number) {
      val += ((Number) payload).doubleValue();
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
