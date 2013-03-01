/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import java.util.HashMap;
import org.apache.commons.lang.mutable.MutableInt;

/**
 * A sink implementation to collect expected test results in a HashMap
 */
public class HashTestSink<T> implements Sink<T>
{
  public HashMap<T, MutableInt> map = new HashMap<T, MutableInt>();
  public int count = 0;

  /**
   *
   * @param payload
   */
  public void clear()
  {
    this.map.clear();
    this.count = 0;
  }

  public int size()
  {
    return map.size();
  }

  public int getCount(T key)
  {
    int ret = -1;
    MutableInt val = map.get(key);
    if (val != null)
    {
      ret = val.intValue();
    }
    return ret;
  }

  @Override
  public void process(T tuple)
  {
    if (tuple instanceof Tuple) {
    }
    else {
      this.count++;
      MutableInt val = map.get(tuple);
      if (val == null) {
        val = new MutableInt(0);
        map.put(tuple, val);
      }
      val.increment();
    }
  }
}
