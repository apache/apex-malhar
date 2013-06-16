/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 *
 * @param <T>
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class Alert extends BaseOperator
{
  protected long lastAlertTimeStamp = -1;
  protected long inAlertSince = -1;
  protected long lastTupleTimeStamp = -1;
  protected long timeout = 5000; // 5 seconds
  protected long alertFrequency = 0;
  protected long levelOneAlertTime = 0;
  protected long levelTwoAlertTime = 0;
  protected long levelThreeAlertTime = 0;
  protected boolean activated = true;
  @InputPortFieldAnnotation(name = "in", optional = false)
  public final transient DefaultInputPort<Object> in = new DefaultInputPort<Object>(this)
  {
    @Override
    public void process(Object tuple)
    {
      long now = System.currentTimeMillis();
      if (inAlertSince < 0) {
        inAlertSince = now;
      }
      lastTupleTimeStamp = now;
      if (activated && lastAlertTimeStamp + alertFrequency < now) {
        if (inAlertSince >= levelOneAlertTime) {
          alert1.emit(tuple);
        }
        if (inAlertSince >= levelTwoAlertTime) {
          alert2.emit(tuple);
        }
        if (inAlertSince >= levelThreeAlertTime) {
          alert3.emit(tuple);
        }
        lastAlertTimeStamp = now;
      }
    }

  };
  @OutputPortFieldAnnotation(name = "alert1", optional = false)
  public final transient DefaultOutputPort<Object> alert1 = new DefaultOutputPort<Object>(this);
  @OutputPortFieldAnnotation(name = "alert2", optional = true)
  public final transient DefaultOutputPort<Object> alert2 = new DefaultOutputPort<Object>(this);
  @OutputPortFieldAnnotation(name = "alert3", optional = true)
  public final transient DefaultOutputPort<Object> alert3 = new DefaultOutputPort<Object>(this);

  public long getTimeout()
  {
    return timeout;
  }

  public void setTimeout(long timeout)
  {
    this.timeout = timeout;
  }

  public long getAlertFrequency()
  {
    return alertFrequency;
  }

  public void setAlertFrequency(long alertFrequency)
  {
    this.alertFrequency = alertFrequency;
  }

  public long getLevelOneAlertTime()
  {
    return levelOneAlertTime;
  }

  public void setLevelOneAlertTime(long levelOneAlertTime)
  {
    this.levelOneAlertTime = levelOneAlertTime;
  }

  public long getLevelTwoAlertTime()
  {
    return levelTwoAlertTime;
  }

  public void setLevelTwoAlertTime(long levelTwoAlertTime)
  {
    this.levelTwoAlertTime = levelTwoAlertTime;
  }

  public long getLevelThreeAlertTime()
  {
    return levelThreeAlertTime;
  }

  public void setLevelThreeAlertTime(long levelThreeAlertTime)
  {
    this.levelThreeAlertTime = levelThreeAlertTime;
  }

  public boolean isActivated()
  {
    return activated;
  }

  public void setActivated(boolean activated)
  {
    this.activated = activated;
  }

  @Override
  public void endWindow()
  {
    checkTimeout();
  }

  protected void checkTimeout()
  {
    if (System.currentTimeMillis() - lastTupleTimeStamp > timeout) {
      inAlertSince = -1;
      lastAlertTimeStamp = -1;
    }
  }

}
