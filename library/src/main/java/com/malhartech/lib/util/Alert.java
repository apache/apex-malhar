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
public class Alert<T> extends BaseOperator
{
  protected long lastAlertTimeStamp = -1;
  protected long inAlertSince = -1;
  protected long timeout = 5000; // 5 seconds
  protected long alertFrequency = 0;
  protected long levelOneTimeStamp = 0;
  protected long levelTwoTimeStamp = 0;
  protected long levelThreeTimeStamp = 0;
  @InputPortFieldAnnotation(name = "in", optional = false)
  public final transient DefaultInputPort<T> in = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      long now = System.currentTimeMillis();
      if (inAlertSince < 0) {
        inAlertSince = now;
      }
      if (lastAlertTimeStamp + alertFrequency < now) {
        if (inAlertSince >= levelOneTimeStamp) {
          alert1.emit(tuple);
        }
        if (inAlertSince >= levelTwoTimeStamp) {
          alert2.emit(tuple);
        }
        if (inAlertSince >= levelThreeTimeStamp) {
          alert3.emit(tuple);
        }
        lastAlertTimeStamp = now;
      }
    }

  };
  @OutputPortFieldAnnotation(name = "alert1", optional = false)
  public final transient DefaultOutputPort<T> alert1 = new DefaultOutputPort<T>(this);
  @OutputPortFieldAnnotation(name = "alert2", optional = false)
  public final transient DefaultOutputPort<T> alert2 = new DefaultOutputPort<T>(this);
  @OutputPortFieldAnnotation(name = "alert3", optional = false)
  public final transient DefaultOutputPort<T> alert3 = new DefaultOutputPort<T>(this);

  public void setAlertFrequency(long millis)
  {
    alertFrequency = millis;
  }

  public void setLevelOneAlertTimeStamp(long millis)
  {
    levelOneTimeStamp = millis;
  }

  public void setLevelTwoAlertTimeStamp(long millis)
  {
    levelTwoTimeStamp = millis;

  }

  public void setLevelThreeAlertTimeStamp(long millis)
  {
    levelThreeTimeStamp = millis;
  }

  public void setTimeout(long millis)
  {
    timeout = millis;
  }

  @Override
  public void endWindow()
  {
    checkTimeout();
  }

  protected void checkTimeout()
  {
    if (System.currentTimeMillis() - inAlertSince > timeout) {
      inAlertSince = -1;
      lastAlertTimeStamp = -1;
    }
  }

}
