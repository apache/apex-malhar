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
import javax.validation.constraints.Min;

/**
 *
 * @param <T>
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class Alert<T> extends BaseOperator
{
  protected long lastAlertTimeStamp = -1;
  protected long inAlertSince = -1;
  protected long lastTupleTimeStamp = -1;

  @Min(0)
  protected long timeout = 5000; // 5 seconds

  @Min(0)
  protected long alertFrequency = 0;
  protected long levelOneTimeStamp = 0;
  protected long levelTwoTimeStamp = 0;
  protected long levelThreeTimeStamp = 0;

  boolean alertOn = true;

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
      lastTupleTimeStamp = now;
      if (lastAlertTimeStamp + alertFrequency < now) {
        if (inAlertSince >= levelOneTimeStamp) {
          if (alertOn) {
            alert1.emit(tuple);
          }
        }
        if (inAlertSince >= levelTwoTimeStamp) {
          if (alertOn) {
            alert2.emit(tuple);
          }
        }
        if (inAlertSince >= levelThreeTimeStamp) {
          if (alertOn) {
            alert3.emit(tuple);
          }
        }
        lastAlertTimeStamp = now;
      }
    }

  };
  @OutputPortFieldAnnotation(name = "alert1", optional = false)
  public final transient DefaultOutputPort<T> alert1 = new DefaultOutputPort<T>(this);
  @OutputPortFieldAnnotation(name = "alert2", optional = true)
  public final transient DefaultOutputPort<T> alert2 = new DefaultOutputPort<T>(this);
  @OutputPortFieldAnnotation(name = "alert3", optional = true)
  public final transient DefaultOutputPort<T> alert3 = new DefaultOutputPort<T>(this);

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

  public void setAlertOn(boolean flag)
  {
    alertOn = flag;
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
