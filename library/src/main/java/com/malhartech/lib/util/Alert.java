/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class Alert extends BaseOperator
{
  protected long lastAlert = -1;
  protected long alertFrequency = 0;
  @InputPortFieldAnnotation(name = "in", optional = false)
  public final transient DefaultInputPort<Object> in = new DefaultInputPort<Object>(this)
  {
    @Override
    public void process(Object tuple)
    {
      long now = System.currentTimeMillis();
      if (lastAlert + alertFrequency < now) {
        alert.emit(tuple);
        lastAlert = now;
      }
    }

  };
  @OutputPortFieldAnnotation(name = "alert", optional = false)
  public final transient DefaultOutputPort<Object> alert = new DefaultOutputPort<Object>(this);

  public void setAlertFrequency(long millis)
  {
    alertFrequency = millis;
  }
}
