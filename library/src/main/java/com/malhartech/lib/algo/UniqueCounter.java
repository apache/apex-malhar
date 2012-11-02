/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UniqueCounter<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      Integer i = map.get(tuple);
      if (i == null) {
        map.put(cloneKey(tuple), 1);
      }
      else {
        map.put(tuple, i + 1);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>(this);
  /**
   * Bucket counting mechanism.
   * Since we clear the bucket at the beginning of the window, we make this object transient.
   */
  transient HashMap<K, Integer> map;

  @Override
  public void beginWindow(long windowId)
  {
    map = new HashMap<K, Integer>();
  }

  @Override
  public void endWindow()
  {
    count.emit(map);
  }
}
