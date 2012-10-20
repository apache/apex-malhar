/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UniqueCounter<T> extends BaseOperator
{
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      Integer i = map.get(tuple);
      if (i == null) {
        map.put(tuple, 1);
      }
      else {
        map.put(tuple, i + 1);
      }
    }
  };
  public final transient DefaultOutputPort<HashMap<T, Integer>> output = new DefaultOutputPort<HashMap<T, Integer>>(this);
  /**
   * Bucket counting mechanism.
   * Since we clear the bucket at the beginning of the window, we make this object transient.
   */
  transient HashMap<T, Integer> map;

  @Override
  public void beginWindow()
  {
    map = new HashMap<T, Integer>();
  }

  @Override
  public void endWindow()
  {
    output.emit(map);
  }
}
