/**
/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.testbench;

/**
 * A sink implementation to collect expected test results.
 */
public class CountTestSink<T> extends CollectorTestSink<T>
{
  public  int count = 0;

  @Override
  public void clear()
  {
    count = 0;
    super.clear();
  }

  public int getCount()
  {
    return count;
  }

  /**
   *
   * @param payload
   */
  @Override
  public void put(T payload)
  {
      count++;
  }
}
