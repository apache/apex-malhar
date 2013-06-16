/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.testbench;

/**
 * A sink implementation to collect expected test results.
 */
public class CountAndLastTupleTestSink<T> extends CountTestSink<T>
{
  public  Object tuple = null;
  /**
   * clears data
   */

  @Override
  public void clear()
  {
    this.tuple = null;
    super.clear();
  }

  @Override
  public void put(T tuple)
  {
      this.tuple = tuple;
      count++;
  }
}
