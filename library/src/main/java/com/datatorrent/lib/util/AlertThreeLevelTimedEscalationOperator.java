/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class AlertThreeLevelTimedEscalationOperator extends AlertEscalationOperator
{
  protected long levelOneAlertTime = 0;
  protected long levelTwoAlertTime = 0;
  protected long levelThreeAlertTime = 0;
  @OutputPortFieldAnnotation(name = "alert2", optional = true)
  public final transient DefaultOutputPort<Object> alert2 = new DefaultOutputPort<Object>();
  @OutputPortFieldAnnotation(name = "alert3", optional = true)
  public final transient DefaultOutputPort<Object> alert3 = new DefaultOutputPort<Object>();

  @Override
  public void processTuple(Object tuple)
  {
    if (inAlertSince >= levelOneAlertTime) {
      alert.emit(tuple);
    }
    if (inAlertSince >= levelTwoAlertTime) {
      alert2.emit(tuple);
    }
    if (inAlertSince >= levelThreeAlertTime) {
      alert3.emit(tuple);
    }
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

}
