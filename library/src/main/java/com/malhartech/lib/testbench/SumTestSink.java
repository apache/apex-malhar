/**
/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;

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
  public void process(T payload)
  {
    if (payload instanceof Tuple) {
    }
    else if (payload instanceof Number) {
      val += ((Number) payload).doubleValue();
    }
  }
}
