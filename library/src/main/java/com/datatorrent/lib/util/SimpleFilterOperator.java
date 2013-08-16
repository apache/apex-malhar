/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class SimpleFilterOperator
{
  @InputPortFieldAnnotation(name = "in", optional = false)
  public final transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      if (satisfiesFilter(tuple)) {
        out.emit(tuple);
      }
    }

  };
  @OutputPortFieldAnnotation(name = "out", optional = false)
  public final transient DefaultOutputPort<Object> out = new DefaultOutputPort<Object>();

  public boolean satisfiesFilter(Object tuple)
  {
    return false;
  }
}
